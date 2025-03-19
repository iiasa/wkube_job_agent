package services

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/iiasa/wkube-job-agent/config"
)

// remoteCopy copies files from a source prefix to a destination directory.
func remoteCopy(source, destination string) error {
	files, err := config.EnumerateFilesByPrefix(source)
	if err != nil {
		return fmt.Errorf("error enumerating files- %v", err) // Stop program on error
	}

	if len(files) > 1 && !strings.HasSuffix(destination, "/") {
		return fmt.Errorf(
			"error: mapping: %s:%s -- destination should end with '/' when mapping is from remote folder with multiple files. ",
			source, destination)
	}

	for _, file := range files {

		var destinationFile string

		if strings.HasSuffix(destination, "/") {
			// Construct the destination file path
			relPath := strings.TrimPrefix(file, source)

			relPath = strings.TrimPrefix(relPath, "/")

			destinationFile = filepath.Join(destination, relPath)
		} else {
			destinationFile = destination
		}

		// Ensure the destination directory exists
		if err := os.MkdirAll(filepath.Dir(destinationFile), os.ModePerm); err != nil {
			return fmt.Errorf("error creating directory: %v", err)
		}

		// Download the file
		fmt.Fprintf(config.MultiLogWriter, "Downloading file: %s\n", file)
		if err := config.DownloadFileFromRepo(file, destinationFile); err != nil {
			return fmt.Errorf("error downloading file: %v", err)
		}
	}

	return nil
}

func remotePush(source, destination string) error {

	destination = strings.TrimRight(destination, string(os.PathSeparator))

	info, err := os.Stat(source)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		if err := config.UploadFile(source, destination); err != nil {
			return err
		}
		return nil
	}

	return filepath.WalkDir(source, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}

		// Join paths safely, ensuring no double slashes
		destPath := filepath.Join(destination, relPath)

		if err := config.UploadFile(path, destPath); err != nil {
			return err
		}
		return nil
	})

	// if err := config.UploadFile(source, destination); err != nil {
	// 	return err
	// }
	// return nil
}

// inputMappingFromMountedStorage creates a symlink from source to destination.
func inputMappingFromMountedStorage(source, destination string) error {

	// Check if the source exists
	if _, err := os.Stat(source); os.IsNotExist(err) {
		return fmt.Errorf("error: source does not exist: %s", source)
	}

	// Ensure the destination directory exists
	if err := os.MkdirAll(filepath.Dir(destination), os.ModePerm); err != nil {
		return fmt.Errorf("error creating directory: %v", err)
	}

	// Remove the destination if it already exists
	if _, err := os.Stat(destination); err == nil {
		if err := os.Remove(destination); err != nil {
			return fmt.Errorf("error removing existing destination: %v", err)
		}
	}

	// Create the symlink
	if err := os.Symlink(source, destination); err != nil {
		return fmt.Errorf("error creating symlink: %v", err)
	}

	return nil
}

func outputMappingFromMountedStorage(source, destination string) error {

	if strings.HasSuffix(source, "/") {

		if err := os.Symlink(source, destination); err != nil {
			return fmt.Errorf("error creating symlink: %v", err)
		}
	} else {

		sourceDir := filepath.Dir(source)
		if err := os.Symlink(sourceDir, destination); err != nil {
			return fmt.Errorf("error creating symlink: %v", err)
		}
	}

	return nil
}

func processInputMappings(inputMappings []string) ([]func() error, error) {

	var taskQueue []func() error

	for _, inputMapping := range inputMappings {
		inputMapping = strings.TrimSpace(inputMapping)
		if inputMapping == "" {
			continue
		}

		inputMappingNew := strings.Replace(inputMapping, "acc://", "__acc__", 1)
		splittedInputMapping := strings.Split(inputMappingNew, ":")
		if len(splittedInputMapping) != 2 {
			return nil, fmt.Errorf("error: invalid input mapping syntax")
		}

		source := splittedInputMapping[0]
		destination := splittedInputMapping[1]

		if !strings.HasPrefix(source, "__acc__") && !strings.HasPrefix(source, "/mnt/data") && source != "selected_files" && source != "selected_folders" {
			return nil, fmt.Errorf("error: invalid source in input mappings")
		}

		if source == "selected_folders" {
			if destination == "" {
				return nil, fmt.Errorf("error: destination for selected_folders mapping should be defined")
			}

			selectedFoldersFromEnv := os.Getenv("selected_foldernames")

			if selectedFoldersFromEnv != "" {
				selectedFolders := strings.Split(selectedFoldersFromEnv, ",")

				var newMappings []string

				for _, selectedFolder := range selectedFolders {

					if selectedFolder != "" {
						newMapping := fmt.Sprintf("acc://%s:%s", selectedFolder, destination)
						newMappings = append(newMappings, newMapping)
					}
				}

				nestedSelectedFolderTaskQueue, err := processInputMappings(newMappings)

				if err != nil {
					return nil, err
				}

				taskQueue = append(taskQueue, nestedSelectedFolderTaskQueue...)
			}

		}

		if source == "selected_files" {
			// first of all destination should not be empty
			// if the destination ends with / then move all the files to that folder also check what happens in other cases
			// if ends with not / and too many files selected -- raise error only one file should be selected

			if destination == "" {
				return nil, fmt.Errorf("error: destination for selected_files mapping should be defined")
			}

			selectedFilesFromEnv := os.Getenv("selected_filenames")

			if selectedFilesFromEnv != "" {
				selectedFiles := strings.Split(selectedFilesFromEnv, ",")

				var newMappings []string

				if strings.HasSuffix(destination, "/") {

					for _, selectedFile := range selectedFiles {

						if selectedFile != "" {
							splittedPath := strings.Split(selectedFile, "/")
							filename := splittedPath[len(splittedPath)-1]
							newDestination := fmt.Sprintf("%s/%s", destination, filename)
							newMapping := fmt.Sprintf("acc://%s:%s", selectedFile, newDestination)
							newMappings = append(newMappings, newMapping)
						}
					}
				} else {

					if len(selectedFiles) > 1 {
						return nil, fmt.Errorf("error: when destination is file (without '/'), there should only be one selected file")
					} else {
						if selectedFiles[0] != "" {
							newMapping := fmt.Sprintf("acc://%s:%s", selectedFiles[0], destination)
							newMappings = append(newMappings, newMapping)
						}
					}
				}

				nestedSelectedfileTaskQueue, err := processInputMappings(newMappings)

				if err != nil {
					return nil, err
				}

				taskQueue = append(taskQueue, nestedSelectedfileTaskQueue...)
			}
		}

		if destination == "" {
			if strings.HasPrefix(source, "__acc__") {
				destination = "/" + strings.TrimPrefix(source, "__acc__")
			}
		}

		if !strings.HasPrefix(destination, "/") {
			return nil, fmt.Errorf("error: invalid destination path: always use absolute path")
		}

		if strings.HasPrefix(source, "__acc__") {
			source = strings.TrimPrefix(source, "__acc__")
			taskQueue = append(taskQueue, func() error {
				if err := remoteCopy(source, destination); err != nil {
					return err
				}
				return nil
			})
		} else if strings.HasPrefix(source, "/mnt/data") {
			taskQueue = append(taskQueue, func() error {
				if err := inputMappingFromMountedStorage(source, destination); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return taskQueue, nil
}

func preProcessOutputMappings(outputMappings []string) ([]func() error, error) {
	var taskQueue []func() error
	// Process output mappings
	for _, outputMapping := range outputMappings {
		outputMapping = strings.TrimSpace(outputMapping)
		if outputMapping == "" {
			continue
		}

		outputMappingNew := strings.Replace(outputMapping, "acc://", "__acc__", 1)
		splittedOutputMapping := strings.Split(outputMappingNew, ":")
		if len(splittedOutputMapping) != 2 {
			return nil, fmt.Errorf("error: invalid output mapping syntax")
		}

		source := splittedOutputMapping[0]
		destination := splittedOutputMapping[1]

		if strings.HasPrefix(source, "__acc__") {
			return nil, fmt.Errorf("error: invalid source in output mappings")
		}

		if !strings.HasPrefix(source, "/") {
			return nil, fmt.Errorf("error: please use absolute URI for source")
		}

		if !strings.HasPrefix(destination, "__acc__") && !strings.HasPrefix(destination, "/mnt/data") {
			return nil, fmt.Errorf("error: invalid destination in output mappings")
		}

		if destination == "" {
			destination = "__acc__" + source
		}

		if strings.HasPrefix(destination, "/mnt/data") {
			taskQueue = append(taskQueue, func() error {
				if err := outputMappingFromMountedStorage(destination, source); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return taskQueue, nil
}

func postProcessOutputMappings(outputMappings []string) ([]func() error, error) {
	var taskQueue []func() error
	// Process output mappings
	for _, outputMapping := range outputMappings {
		outputMapping = strings.TrimSpace(outputMapping)
		if outputMapping == "" {
			continue
		}

		outputMappingNew := strings.Replace(outputMapping, "acc://", "__acc__", 1)
		splittedOutputMapping := strings.Split(outputMappingNew, ":")
		if len(splittedOutputMapping) != 2 {
			return nil, fmt.Errorf("error: invalid output mapping syntax")
		}

		source := splittedOutputMapping[0]
		destination := splittedOutputMapping[1]

		if strings.HasPrefix(source, "__acc__") {
			return nil, fmt.Errorf("error: invalid source in output mappings")
		}

		if !strings.HasPrefix(source, "/") {
			return nil, fmt.Errorf("error: please use absolute URI for source")
		}

		if !strings.HasPrefix(destination, "__acc__") && !strings.HasPrefix(destination, "/mnt/data") {
			return nil, fmt.Errorf("error: invalid destination in output mappings")
		}

		if destination == "" {
			destination = "__acc__" + source
		}

		if strings.HasPrefix(destination, "__acc__") {
			destination = strings.TrimPrefix(destination, "__acc__")
			taskQueue = append(taskQueue, func() error {
				if err := remotePush(source, destination); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return taskQueue, nil
}

func PreProcessMappings() error {

	fmt.Fprintln(config.MultiLogWriter, "Pre process input/output mappings started")

	inputMappings := os.Getenv("input_mappings")
	outputMappings := os.Getenv("output_mappings")

	if inputMappings == "" || outputMappings == "" {
		return fmt.Errorf("error: input_mappings or output_mappings environment variables not set")
	}

	allInputMappings := strings.Split(inputMappings, ";")
	allOutputMappings := strings.Split(outputMappings, ";")

	inputMappingsTaskQueue, err := processInputMappings(allInputMappings)

	if err != nil {
		return fmt.Errorf("error: error preparing input mappings")
	}

	outputMappingsTaskQueue, err := preProcessOutputMappings(allOutputMappings)

	if err != nil {
		return fmt.Errorf("error: error preparing pre processing task queue")
	}

	taskQueue := append(inputMappingsTaskQueue, outputMappingsTaskQueue...)

	errChan := make(chan error, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, task := range taskQueue {
			err := task()
			if err != nil {
				errChan <- err
				close(errChan)
				return
			}
		}
	}()
	wg.Wait()

	fmt.Fprintln(config.MultiLogWriter, "Pre process input/output mappings completed")

	return nil
}

func PostProcessMappings() error {
	fmt.Fprintln(config.MultiLogWriter, "Post process output mappings started ")

	outputMappings := os.Getenv("output_mappings")

	allOutputMappings := strings.Split(outputMappings, ";")

	taskQueue, err := postProcessOutputMappings(allOutputMappings)

	if err != nil {
		return fmt.Errorf("error: error preparing post processing task queue")
	}

	errChan := make(chan error, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, task := range taskQueue {
			err := task()
			if err != nil {
				errChan <- err
				close(errChan)
				return
			}
		}
	}()
	wg.Wait()

	if err := <-errChan; err != nil {
		return fmt.Errorf("post processing failed: %w", err)
	}

	fmt.Fprintln(config.MultiLogWriter, "Post process output mappings completed")

	return nil
}
