package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// remoteCopy copies files from a source prefix to a destination directory.
func remoteCopy(source, destination string) {
	files, err := enumerateFilesByPrefix(source)
	if err != nil {
		log.Fatalf("Error enumerating files: %v\n", err) // Stop program on error
	}

	for _, file := range files {

		var destinationFile string

		if strings.HasSuffix(destination, "/") {
			// Construct the destination file path
			relPath := strings.TrimPrefix(file, source)
			destinationFile = filepath.Join(destination, relPath)
		} else {
			destinationFile = destination
		}

		// Ensure the destination directory exists
		if err := os.MkdirAll(filepath.Dir(destinationFile), os.ModePerm); err != nil {
			log.Fatalf("Error creating directory: %v\n", err) // Stop program on error
		}

		// Download the file
		fmt.Printf("Downloading file: %s\n", file)
		if err := downloadFileFromRepo(file, destinationFile); err != nil {
			log.Fatalf("Error downloading file: %v\n", err) // Stop program on error
		}
	}
}

// inputMappingFromMountedStorage creates a symlink from source to destination.
func inputMappingFromMountedStorage(source, destination string) {

	// Check if the source exists
	if _, err := os.Stat(source); os.IsNotExist(err) {
		log.Fatalf("Error: source does not exist: %s\n", source) // Stop program on error
	}

	// Ensure the destination directory exists
	if err := os.MkdirAll(filepath.Dir(destination), os.ModePerm); err != nil {
		log.Fatalf("Error creating directory: %v\n", err) // Stop program on error
	}

	// Remove the destination if it already exists
	if _, err := os.Stat(destination); err == nil {
		if err := os.Remove(destination); err != nil {
			log.Fatalf("Error removing existing destination: %v\n", err) // Stop program on error
		}
	}

	// Create the symlink
	if err := os.Symlink(source, destination); err != nil {
		log.Fatalf("Error creating symlink: %v\n", err) // Stop program on error
	}
}

// outputMappingFromMountedStorage creates a symlink for output mappings.
func outputMappingFromMountedStorage(source, destination string) {

	// Check if the source is a directory
	if strings.HasSuffix(source, "/") {
		// It's a directory, create a symlink to the directory
		if err := os.Symlink(source, destination); err != nil {
			log.Fatalf("Error creating symlink: %v\n", err) // Stop program on error
		}
	} else {
		// It's a file, symlink to the directory containing the file
		sourceDir := filepath.Dir(source)
		if err := os.Symlink(sourceDir, destination); err != nil {
			log.Fatalf("Error creating symlink: %v\n", err) // Stop program on error
		}
	}
}

func processInputMappings(inputMappings []string) {

	taskQueue := []func(){}

	for _, inputMapping := range inputMappings {
		inputMapping = strings.TrimSpace(inputMapping)
		if inputMapping == "" {
			continue
		}

		inputMappingNew := strings.Replace(inputMapping, "acc://", "__acc__", 1)
		splittedInputMapping := strings.Split(inputMappingNew, ":")
		if len(splittedInputMapping) != 2 {
			log.Fatal("Error: invalid input mapping syntax") // Stop program on error
		}

		source := splittedInputMapping[0]
		destination := splittedInputMapping[1]

		if !strings.HasPrefix(source, "__acc__") && !strings.HasPrefix(source, "/mnt/data") && source != "selected_files" {
			log.Fatal("Error: invalid source in input mappings") // Stop program on error
		}

		if source == "selected_files" {
			// first of all destination should not be empty
			// if the destination ends with / then move all the files to that folder also check what happens in other cases
			// if ends with not / and too many files selected -- raise error only one file should be selected

			if destination == "" {
				log.Fatal("Error: destination for selected_files mapping should be defined.")
			}

			selectedFilesFromEnv = os.Getenv("selected_filenames")

			if selectedFilesFromEnv != "" {
				selectedFiles := strings.Split(selectedFilesFromEnv, ",")

				if strings.HasSuffix(destination, '/') {

				} else {

				}
			}

		}

		if destination == "" {
			if strings.HasPrefix(source, "__acc__") {
				destination = "/" + strings.TrimPrefix(source, "__acc__")
			}
		}

		if !strings.HasPrefix(destination, "/") {
			log.Fatal("Error: invalid destination path: always use absolute path") // Stop program on error
		}

		if strings.HasPrefix(source, "__acc__") {
			source = strings.TrimPrefix(source, "__acc__")
			append(taskQueue, func() { remoteCopy(source, destination) })
		} else if strings.HasPrefix(source, "/mnt/data") {
			append(taskQueue, func() { inputMappingFromMountedStorage(source, destination) })
		}
	}
	return taskQueue
}

func processOutputMappings(outputMappings []string) {
	taskQueue := []func(){}
	// Process output mappings
	for _, outputMapping := range outputMappings {
		outputMapping = strings.TrimSpace(outputMapping)
		if outputMapping == "" {
			continue
		}

		outputMappingNew := strings.Replace(outputMapping, "acc://", "__acc__", 1)
		splittedOutputMapping := strings.Split(outputMappingNew, ":")
		if len(splittedOutputMapping) != 2 {
			log.Fatal("Error: invalid output mapping syntax") // Stop program on error
		}

		source := splittedOutputMapping[0]
		destination := splittedOutputMapping[1]

		if strings.HasPrefix(source, "__acc__") {
			log.Fatal("Error: invalid source in output mappings") // Stop program on error
		}

		if !strings.HasPrefix(source, "/") {
			log.Fatal("Error: please use absolute URI for source") // Stop program on error
		}

		if !strings.HasPrefix(destination, "__acc__") && !strings.HasPrefix(destination, "/mnt/data") {
			log.Fatal("Error: invalid destination in output mappings") // Stop program on error
		}

		if destination == "" {
			destination = "__acc__" + source
		}

		if strings.HasPrefix(destination, "/mnt/data") {
			append(taskQueue, func() { outputMappingFromMountedStorage(destination, source) })
		}
	}
	return taskQueue
}

func processMappings() {

	// Retrieve input and output mappings from environment variables
	inputMappings := os.Getenv("input_mappings")
	outputMappings := os.Getenv("output_mappings")

	if inputMappings == "" || outputMappings == "" {
		log.Fatal("Error: input_mappings or output_mappings environment variables not set") // Stop program on error
	}

	// Split mappings into individual tasks
	allInputMappings := strings.Split(inputMappings, ";")
	allOutputMappings := strings.Split(outputMappings, ";")

	var wg sync.WaitGroup
	// Process input mappings
	inputMappingsTaskQueue := processInputMappings(allInputMappings)
	outputMappingsTaskQueue := processOutputMappings(allOutputMappings)

	taskQueue := append(inputMappingsTaskQueue, outputMappingsTaskQueue...)

	// Wait for all tasks to complete
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, task := range taskQueue {
			task()
		}
	}()
	wg.Wait()

	fmt.Println("Input mappings completed completed")
}
