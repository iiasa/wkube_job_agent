package services

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func remoteCopy(source, destination string) error {
	files, err := EnumerateFilesByPrefix(source)
	if err != nil {
		return fmt.Errorf("error enumerating files- %v", err)
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
		fmt.Fprintf(MultiLogWriter, "Downloading file: %s\n", file)
		if err := DownloadFileFromRepo(file, destinationFile); err != nil {
			return fmt.Errorf("error downloading file: %v", err)
		}
	}

	return nil
}

func graphStorageCopy(source, destination string) error {
	srcInfo, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("source error: %w", err)
	}

	if !srcInfo.IsDir() {
		return fmt.Errorf("source is not a directory")
	}

	// Create destination if it doesn't exist
	if _, err := os.Stat(destination); os.IsNotExist(err) {
		if err := os.MkdirAll(destination, srcInfo.Mode()); err != nil {
			return fmt.Errorf("failed to create destination: %w", err)
		}
	}

	// Walk through the source
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}

		destPath := filepath.Join(destination, relPath)

		if info.IsDir() {
			return os.MkdirAll(destPath, info.Mode())
		}

		// Copy file
		return copyFile(path, destPath, info.Mode())
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("copy failed: %w", err)
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
		if err := UploadFile(source, destination); err != nil {
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

		if err := UploadFile(path, destPath); err != nil {
			return err
		}
		return nil
	})

	// if err := UploadFile(source, destination); err != nil {
	// 	return err
	// }
	// return nil
}

func inputMappingFromMountedStorage(source, destination string) error {

	if _, err := os.Stat(source); os.IsNotExist(err) {

		if strings.HasSuffix(source, "/") {

			err := os.MkdirAll(source, 0775)
			if err != nil {
				return fmt.Errorf("error: creating directory for data mapping from mounted storage '%s': %w", source, err)
			}
		} else {

			return fmt.Errorf("error: file for data mounting from mounted storage does not exists: %s", source)
		}

	}

	// Ensure destination's parent directory exists
	if err := os.MkdirAll(filepath.Dir(destination), 0775); err != nil {
		return fmt.Errorf("error creating parent directory: %v", err)
	}

	// Check if destination exists
	if info, err := os.Lstat(destination); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			// Destination is a symlink — throw an error
			return fmt.Errorf("error: destination '%s' is already a symlink — conflict. There must be identical mapping. or a already existing symlink in the job container", destination)
		} else {
			// Not a symlink — remove it
			if err := os.RemoveAll(destination); err != nil {
				return fmt.Errorf("error removing existing non-symlink destination '%s': %v", destination, err)
			}
		}
	} else if !os.IsNotExist(err) {
		// Some other error accessing destination
		return fmt.Errorf("error checking destination '%s': %v", destination, err)
	}

	destination = strings.TrimRight(destination, "/")

	// Create the symlink
	if err := os.Symlink(source, destination); err != nil {
		return fmt.Errorf("error creating symlink: %v", err)
	}

	// Check for symlink loop
	if _, err := filepath.EvalSymlinks(destination); err != nil {
		// Remove the symlink if a loop is detected
		_ = os.Remove(destination)
		if strings.Contains(err.Error(), "too many links") {
			return fmt.Errorf("symlink loop detected: %v", err)
		}
		return fmt.Errorf("error resolving symlink after creation: %v", err)
	}

	return nil
}

func outputMappingToMountedStorage(source, destination string) error {

	fmt.Printf("Performing output mapping to mounted source using inputMappingFromMountedStorage by switching source and destination.\n")

	// Same logic as input mapping as it will be done together that is before the job states.
	// source and destination with respect to output mapping has already been changed by callee
	return inputMappingFromMountedStorage(source, destination)
}

func processInputMappings(inputMappings []string) ([]func() error, []func() error, error) {

	var taskQueue []func() error

	var symlinkQueue []func() error

	for _, inputMapping := range inputMappings {
		inputMapping = strings.TrimSpace(inputMapping)
		if inputMapping == "" {
			continue
		}

		inputMappingNew := strings.Replace(inputMapping, "acc://", "__acc__", 1)
		splittedInputMapping := strings.Split(inputMappingNew, ":")
		if len(splittedInputMapping) != 2 {
			return nil, nil, fmt.Errorf("error: invalid input mapping syntax")
		}

		source := splittedInputMapping[0]
		destination := splittedInputMapping[1]

		if !strings.HasPrefix(source, "__acc__") &&
			!strings.HasPrefix(source, "/mnt/pipe") &&
			!strings.HasPrefix(source, "/mnt/graph") &&
			source != "selected_files" &&
			source != "selected_folders" {
			return nil, nil, fmt.Errorf("error: invalid source in input mappings")
		}

		if source == "selected_folders" {
			if destination == "" {
				return nil, nil, fmt.Errorf("error: destination for selected_folders mapping should be defined")
			}

			selectedFoldersFromEnv := os.Getenv("selected_foldernames")

			if selectedFoldersFromEnv == "" {
				return nil, nil, fmt.Errorf("error: selected_folders referenced in source no folder selection detected")
			} else {
				selectedFolders := strings.Split(selectedFoldersFromEnv, ",")

				var newMappings []string

				for _, selectedFolder := range selectedFolders {

					if selectedFolder != "" {
						newMapping := fmt.Sprintf("acc://%s:%s", selectedFolder, destination)
						newMappings = append(newMappings, newMapping)
					}
				}

				nestedSelectedFolderTaskQueue, _, err := processInputMappings(newMappings)

				if err != nil {
					return nil, nil, err
				}

				taskQueue = append(taskQueue, nestedSelectedFolderTaskQueue...)
			}

		}

		if source == "selected_files" {
			// first of all destination should not be empty
			// if the destination ends with / then move all the files to that folder also check what happens in other cases
			// if ends with not / and too many files selected -- raise error only one file should be selected

			if destination == "" {
				return nil, nil, fmt.Errorf("error: destination for selected_files mapping should be defined")
			}

			selectedFilesFromEnv := os.Getenv("selected_filenames")

			if selectedFilesFromEnv == "" {
				return nil, nil, fmt.Errorf("error: selected_files referenced in source no file selection detected")
			} else {
				selectedFiles := strings.Split(selectedFilesFromEnv, ",")

				var newMappings []string

				if strings.HasSuffix(destination, "/") {

					for _, selectedFile := range selectedFiles {

						if selectedFile != "" {
							newDestination := fmt.Sprintf("%s%s", destination, selectedFile)
							newMapping := fmt.Sprintf("acc://%s:%s", selectedFile, newDestination)
							newMappings = append(newMappings, newMapping)
						}
					}
				} else {

					if len(selectedFiles) > 1 {
						return nil, nil, fmt.Errorf("error: when destination is file (without '/'), there should only be one selected file")
					} else {
						if selectedFiles[0] != "" {
							newMapping := fmt.Sprintf("acc://%s:%s", selectedFiles[0], destination)
							newMappings = append(newMappings, newMapping)
						}
					}
				}

				nestedSelectedfileTaskQueue, _, err := processInputMappings(newMappings)

				if err != nil {
					return nil, nil, err
				}

				taskQueue = append(taskQueue, nestedSelectedfileTaskQueue...)
			}
		}

		if destination == "" {
			if strings.HasPrefix(source, "__acc__") {
				destination = "/" + strings.TrimPrefix(source, "__acc__")
			}
		}

		if strings.HasSuffix(destination, "/*") {
			if strings.HasPrefix(source, "__acc__") {
				destination = strings.TrimSuffix(destination, "/*") + "/" + strings.TrimPrefix(source, "__acc__")
			}
		}

		if !strings.HasPrefix(destination, "/") {
			return nil, nil, fmt.Errorf("error: invalid destination path: always use absolute path")
		}

		if strings.HasPrefix(source, "/mnt/pipe") {
			symlinkQueue = append(taskQueue, func() error {
				if err := inputMappingFromMountedStorage(source, destination); err != nil {
					return err
				}
				return nil
			})
		} else if strings.HasPrefix(source, "/mnt/graph") {
			taskQueue = append(taskQueue, func() error {
				if err := graphStorageCopy(source, destination); err != nil {
					return err
				}
				return nil
			})
		} else if strings.HasPrefix(source, "__acc__") {
			source = strings.TrimPrefix(source, "__acc__")
			taskQueue = append(taskQueue, func() error {
				if err := remoteCopy(source, destination); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return taskQueue, symlinkQueue, nil
}

func preProcessOutputMappings(outputMappings []string) ([]func() error, error) {
	var symlinkQueue []func() error
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

		if !strings.HasPrefix(destination, "__acc__") &&
			!strings.HasPrefix(destination, "/mnt/pipe") &&
			!strings.HasPrefix(destination, "/mnt/graph") {
			return nil, fmt.Errorf("error: invalid destination in output mappings")
		}

		if destination == "" {
			destination = "__acc__" + source
		}

		if strings.HasPrefix(destination, "/mnt/pipe") {
			symlinkQueue = append(symlinkQueue, func() error {
				if err := outputMappingToMountedStorage(destination, source); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return symlinkQueue, nil
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

		if !strings.HasPrefix(destination, "__acc__") &&
			!strings.HasPrefix(destination, "/mnt/pipe") &&
			!strings.HasPrefix(destination, "/mnt/graph") {
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
		} else if strings.HasPrefix(destination, "/mnt/graph") {
			taskQueue = append(taskQueue, func() error {
				if err := graphStorageCopy(source, destination); err != nil {
					return err
				}
				return nil
			})
		}
	}
	return taskQueue, nil
}

func PreProcessMappings() error {

	fmt.Fprintln(MultiLogWriter, "Pre process input/output mappings started")

	inputMappings := os.Getenv("input_mappings")
	inputMappings = os.ExpandEnv(inputMappings)
	outputMappings := os.Getenv("output_mappings")
	outputMappings = os.ExpandEnv(outputMappings)

	allInputMappings := strings.Split(inputMappings, ";")
	allOutputMappings := strings.Split(outputMappings, ";")

	taskQueue, symlinkQueueFromInputMapping, err := processInputMappings(allInputMappings)

	if err != nil {
		return fmt.Errorf("error: error preparing input mappings %v", err)
	}

	symlinkQueueFromOutputMapping, err := preProcessOutputMappings(allOutputMappings)

	if err != nil {
		return fmt.Errorf("error: error preparing pre processing task queue %v", err)
	}

	var finalTaskQueue []func() error
	finalTaskQueue = append(finalTaskQueue, symlinkQueueFromInputMapping...)
	finalTaskQueue = append(finalTaskQueue, symlinkQueueFromOutputMapping...)
	finalTaskQueue = append(finalTaskQueue, taskQueue...)

	errChan := make(chan error, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, task := range finalTaskQueue {
			err := task()
			if err != nil {
				errChan <- err
				close(errChan)
				return
			}
		}
	}()
	wg.Wait()

	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("pre process input/output mappings: %w", err)
		}
	default:
		// No error occurred, continue
	}

	fmt.Fprintln(MultiLogWriter, "Pre process input/output mappings completed")

	return nil
}

func PostProcessMappings() error {
	fmt.Fprintln(MultiLogWriter, "Post process output mappings started ")

	outputMappings := os.Getenv("output_mappings")
	outputMappings = os.ExpandEnv(outputMappings)

	allOutputMappings := strings.Split(outputMappings, ";")

	taskQueue, err := postProcessOutputMappings(allOutputMappings)

	if err != nil {
		return fmt.Errorf("error: error preparing post processing task queue %v", err)
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

	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("post processing failed: %w", err)
		}
	default:
		// No error occurred, continue
	}

	fmt.Fprintln(MultiLogWriter, "Post process output mappings completed")

	return nil
}
