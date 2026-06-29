package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type downloadFileInfo struct {
	DestinationPath string `json:"destination_path"`
	Hash            string `json:"hash"`
	FileSize        int64  `json:"file_size"`
}

type FileStat struct {
	ObjectName string `json:"object_name"`
	Size       int64  `json:"size"`
	MerkleHash string `json:"merkle_hash"`
}

func getFileStat(filename string) (*FileStat, error) {
	projectSlug := GetProjectSlug()

	endpoint := fmt.Sprintf("/%s/file-stat/", projectSlug)

	payload := map[string]string{
		"filename": filename,
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := CreateRequest("POST", endpoint, bodyBytes)
	if err != nil {
		return nil, err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return nil, fmt.Errorf("POST %s returned not okay status %v", endpoint, err)
	}

	var result FileStat
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func remoteCopy(source, destination string) error {
	// soure could be a file or folder
	// destination could be a file or folder, if destionation ends with / it is considered as folder otherwise file
	// if soure is folder we should copy all the files one by one to destination appendinf relative filepath from source folder into destination folder.
	// if source is a single file and destination is folder we should copy the file to that folder with same filename
	files, err := EnumerateFilesByPrefix(source)
	if err != nil {
		return fmt.Errorf("error enumerating files- %v", err)
	}

	if len(files) == 0 {
		return nil
	}

	if len(files) > 1 && !strings.HasSuffix(destination, "/") {
		return fmt.Errorf(
			"error: mapping: %s:%s -- destination should end with '/' when mapping is from remote folder with multiple files. ",
			source, destination)
	}

	var downloadList []downloadFileInfo

	for _, file := range files {
		var destinationFile string
		if strings.HasSuffix(destination, "/") {
			relPath := strings.TrimPrefix(file, source)
			relPath = strings.TrimPrefix(relPath, "/")
			if relPath == "" {
				destinationFile = filepath.Join(destination, filepath.Base(file))
			} else {
				destinationFile = filepath.Join(destination, relPath)
			}
		} else {
			destinationFile = destination
		}

		if err := os.MkdirAll(filepath.Dir(destinationFile), os.ModePerm); err != nil {
			return fmt.Errorf("error creating directory: %v", err)
		}

		projectSlug := GetProjectSlug()

		fileParam := strings.TrimPrefix(file, projectSlug+"/")

		stat, err := getFileStat(fileParam)
		if err != nil {
			return fmt.Errorf("error getting stat for %s: %v", file, err)
		}

		downloadList = append(downloadList, downloadFileInfo{
			DestinationPath: destinationFile,
			Hash:            stat.MerkleHash,
			FileSize:        stat.Size,
		})
	}

	filesJSON, err := json.Marshal(downloadList)
	if err != nil {
		return err
	}

	projectSlug := strings.Split(files[0], "/")[0]
	gatewayServer := getenvWithDefault("ACC_JOB_GATEWAY_SERVER", "https://accelerator.iiasa.ac.at")
	casEndpoint := strings.TrimRight(gatewayServer, "/") + "/api/xet-cas"
	accessToken, expiresAt, err := GetAccessToken()
	if err != nil {
		return fmt.Errorf("failed to retrieve access token: %w", err)
	}
	casToken := fmt.Sprintf("xet_session_prj_%s_%s", projectSlug, accessToken)

	args := []string{
		"download",
		casEndpoint,
		casToken,
		fmt.Sprintf("%d", expiresAt),
		string(filesJSON),
	}

	fmt.Fprintf(MultiLogWriter, "Downloading %d files via hf_xet...\n", len(downloadList))
	ctx, cancel := context.WithTimeout(RootCtx, 30*time.Minute)
	defer cancel()
	if err := RunHelperCommand(ctx, args); err != nil {
		return fmt.Errorf("hf_xet download failed: %w", err)
	}

	return nil
}

type uploadFileInfo struct {
	LocalPath  string `json:"local_path"`
	RemotePath string `json:"remote_path"`
}

func remotePush(source, destination string) error {
	info, err := os.Stat(source)
	if err != nil {
		return err
	}

	var uploadList []uploadFileInfo

	if !info.IsDir() {
		var remotePath string
		if strings.HasSuffix(destination, "/") {
			remotePath = filepath.Join(destination, filepath.Base(source))
		} else {
			remotePath = destination
		}
		remotePath = strings.TrimRight(remotePath, string(os.PathSeparator))

		uploadList = append(uploadList, uploadFileInfo{
			LocalPath:  source,
			RemotePath: remotePath,
		})
	} else {
		err = filepath.WalkDir(source, func(path string, d os.DirEntry, err error) error {
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

			destPath := filepath.Join(destination, relPath)
			destPath = strings.TrimRight(destPath, string(os.PathSeparator))
			uploadList = append(uploadList, uploadFileInfo{
				LocalPath:  path,
				RemotePath: destPath,
			})
			return nil
		})
		if err != nil {
			return err
		}
	}

	if len(uploadList) == 0 {
		return nil
	}

	filesJSON, err := json.Marshal(uploadList)
	if err != nil {
		return err
	}

	projectSlug := GetProjectSlug()
	if projectSlug == "" {
		projectSlug = strings.Split(uploadList[0].RemotePath, "/")[0]
	}
	gatewayServer := getenvWithDefault("ACC_JOB_GATEWAY_SERVER", "https://accelerator.iiasa.ac.at")
	casEndpoint := strings.TrimRight(gatewayServer, "/") + "/api/xet-cas"
	accessToken, expiresAt, err := GetAccessToken()
	if err != nil {
		return fmt.Errorf("failed to retrieve access token: %w", err)
	}
	casToken := fmt.Sprintf("xet_session_prj_%s_%s", projectSlug, accessToken)
	registerUrl := strings.TrimRight(gatewayServer, "/") + "/api/xet-cas/v1/cas/bulk-register"

	args := []string{
		"upload",
		projectSlug,
		casEndpoint,
		casToken,
		fmt.Sprintf("%d", expiresAt),
		registerUrl,
		string(filesJSON),
	}

	fmt.Fprintf(MultiLogWriter, "Uploading %d files via hf_xet...\n", len(uploadList))
	ctx, cancel := context.WithTimeout(RootCtx, 30*time.Minute)
	defer cancel()
	if err := RunHelperCommand(ctx, args); err != nil {
		return fmt.Errorf("hf_xet upload failed: %w", err)
	}

	return nil
}

func mergeDirectory(srcDir, destDir string) error {
	return filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		targetPath := filepath.Join(destDir, relPath)

		if d.IsDir() {
			return os.MkdirAll(targetPath, 0775)
		}

		// Check if it already exists in the target mount directory
		if _, err := os.Stat(targetPath); err == nil {
			// Exists at mount, preserve it (skip copying)
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}

		// Copy file content using ReadFile/WriteFile
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(targetPath, data, 0664)
	})
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

	// Clean destination path
	destination = strings.TrimRight(destination, "/")

	// Ensure destination's parent directory exists
	if err := os.MkdirAll(filepath.Dir(destination), 0775); err != nil {
		return fmt.Errorf("error creating parent directory: %v", err)
	}

	// Check if destination exists
	if info, err := os.Lstat(destination); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			// It is a symlink. Check where it points.
			target, err := os.Readlink(destination)
			if err != nil {
				return fmt.Errorf("error reading existing symlink '%s': %v", destination, err)
			}
			if target == source {
				// Already correctly mapped
				return nil
			}
			// Points to a different source, delete it so we can re-create it
			if err := os.Remove(destination); err != nil {
				return fmt.Errorf("error removing existing symlink '%s' pointing to '%s': %v", destination, target, err)
			}
		} else if info.IsDir() {
			// Recursively copy/merge files to the source directory
			if err := mergeDirectory(destination, source); err != nil {
				return fmt.Errorf("error merging pre-existing directory '%s' to '%s': %v", destination, source, err)
			}
			// Safe to delete local files now that they have been merged
			if err := os.RemoveAll(destination); err != nil {
				return fmt.Errorf("error removing merged directory '%s': %v", destination, err)
			}
		} else {
			// It is a regular file. Merge it to the source.
			sourceInfo, err := os.Stat(source)
			if err != nil {
				return fmt.Errorf("error checking source '%s': %v", source, err)
			}
			var targetPath string
			if sourceInfo.IsDir() {
				targetPath = filepath.Join(source, filepath.Base(destination))
			} else {
				targetPath = source
			}

			// Copy if it doesn't exist on the mount
			if _, err := os.Stat(targetPath); os.IsNotExist(err) {
				data, err := os.ReadFile(destination)
				if err != nil {
					return fmt.Errorf("error reading conflicting file '%s': %v", destination, err)
				}
				if err := os.WriteFile(targetPath, data, 0664); err != nil {
					return fmt.Errorf("error merging conflicting file to '%s': %v", targetPath, err)
				}
			}

			// Remove original conflicting file
			if err := os.Remove(destination); err != nil {
				return fmt.Errorf("error removing conflicting file '%s': %v", destination, err)
			}
		}
	} else if !os.IsNotExist(err) {
		// Some other error accessing destination
		return fmt.Errorf("error checking destination '%s': %v", destination, err)
	}

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
			!strings.HasPrefix(source, "/mnt/tmp") &&
			!strings.HasPrefix(source, "/mnt/wdrv") &&
			source != "selected_files" &&
			source != "selected_folders" {
			return nil, nil, fmt.Errorf("error: invalid source in input mappings %s:%s", source, destination)
		}

		if source == "selected_folders" {
			if destination == "" {
				return nil, nil, fmt.Errorf("error: destination for selected_folders mapping should be defined")
			}

			selectedFoldersFromEnv := os.Getenv("selected_foldernames")

			if selectedFoldersFromEnv == "" {
				fmt.Fprintln(MultiLogWriter, "warning: selected_folders referenced in source but no folder selection detected")
				continue
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
				fmt.Fprintln(MultiLogWriter, "warning: selected_files referenced in source but no file selection detected")
				continue
			} else {
				selectedFiles := strings.Split(selectedFilesFromEnv, ",")

				var newMappings []string

				if strings.HasSuffix(destination, "/") {

					for _, selectedFile := range selectedFiles {

						if selectedFile != "" {
							newMapping := fmt.Sprintf("acc://%s:%s", selectedFile, destination)
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

		if !strings.HasPrefix(destination, "/") {
			return nil, nil, fmt.Errorf("error: invalid destination path: always use absolute path")
		}

		if strings.HasPrefix(source, "/mnt/tmp") || strings.HasPrefix(source, "/mnt/wdrv") {
			symlinkQueue = append(symlinkQueue, func() error {
				if err := inputMappingFromMountedStorage(source, destination); err != nil {
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
			!strings.HasPrefix(destination, "/mnt/tmp") &&
			!strings.HasPrefix(destination, "/mnt/wdrv") {
			return nil, fmt.Errorf("error: invalid destination in output mappings")
		}

		if destination == "" {
			destination = "__acc__" + source
		}

		if strings.HasPrefix(destination, "/mnt/tmp") || strings.HasPrefix(destination, "/mnt/wdrv") {
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
			!strings.HasPrefix(destination, "/mnt/tmp") &&
			!strings.HasPrefix(destination, "/mnt/wdrv") {
			return nil, fmt.Errorf("error: invalid destination in output mappings")
		}

		if destination == "" {
			destination = "__acc__" + source
		}

		if strings.HasPrefix(destination, "__acc__") {
			destination = strings.TrimPrefix(destination, "__acc__")
			rootJobID := os.Getenv("ROOT_JOB_ID")
			if rootJobID != "" {
				destination = fmt.Sprintf("job-outputs/%s/%s", rootJobID, strings.TrimPrefix(destination, "/"))
			}
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

func GetProjectSlug() string {
	// 1. Try environment variable PROJECT_SLUG
	if slug := os.Getenv("PROJECT_SLUG"); slug != "" {
		return slug
	}
	// 2. Try environment variable project_slug
	if slug := os.Getenv("project_slug"); slug != "" {
		return slug
	}
	// 3. Try environment variable ACC_PROJECT_SLUG
	if slug := os.Getenv("ACC_PROJECT_SLUG"); slug != "" {
		return slug
	}

	return ""
}

func RemotePushLog(source, destination, projectSlug string) error {
	jobID := os.Getenv("JOB_ID")
	if jobID != "" {
		destination = fmt.Sprintf("job-outputs/%s/%s", jobID, strings.TrimPrefix(destination, "/"))
	}
	destination = strings.TrimRight(destination, string(os.PathSeparator))

	info, err := os.Stat(source)
	if err != nil {
		return err
	}

	var uploadList []uploadFileInfo

	if !info.IsDir() {
		uploadList = append(uploadList, uploadFileInfo{
			LocalPath:  source,
			RemotePath: destination,
		})
	} else {
		err = filepath.WalkDir(source, func(path string, d os.DirEntry, err error) error {
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

			destPath := filepath.Join(destination, relPath)
			uploadList = append(uploadList, uploadFileInfo{
				LocalPath:  path,
				RemotePath: destPath,
			})
			return nil
		})
		if err != nil {
			return err
		}
	}

	if len(uploadList) == 0 {
		return nil
	}

	filesJSON, err := json.Marshal(uploadList)
	if err != nil {
		return err
	}

	gatewayServer := getenvWithDefault("ACC_JOB_GATEWAY_SERVER", "https://accelerator.iiasa.ac.at")
	casEndpoint := strings.TrimRight(gatewayServer, "/") + "/api/xet-cas"
	accessToken, expiresAt, err := GetAccessToken()
	if err != nil {
		return fmt.Errorf("failed to retrieve access token: %w", err)
	}
	casToken := fmt.Sprintf("xet_session_prj_%s_%s", projectSlug, accessToken)
	registerUrl := strings.TrimRight(gatewayServer, "/") + "/api/xet-cas/v1/cas/bulk-register"

	args := []string{
		"upload",
		projectSlug,
		casEndpoint,
		casToken,
		fmt.Sprintf("%d", expiresAt),
		registerUrl,
		string(filesJSON),
	}

	fmt.Fprintf(MultiLogWriter, "Uploading %d log files via hf_xet...\n", len(uploadList))
	ctx, cancel := context.WithTimeout(RootCtx, 30*time.Minute)
	defer cancel()
	if err := RunHelperCommand(ctx, args); err != nil {
		return fmt.Errorf("hf_xet log upload failed: %w", err)
	}

	return nil
}

func UploadWdrvFilesCreatedByJobUid() error {
	jobUidStr := os.Getenv("JOB_UID")
	if jobUidStr == "" {
		return nil // No JOB_UID set, skip
	}
	jobUid, err := strconv.ParseUint(jobUidStr, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid JOB_UID %s: %v", jobUidStr, err)
	}

	projectSlug := GetProjectSlug()
	if projectSlug == "" {
		return fmt.Errorf("project_slug is empty")
	}

	var uploadList []uploadFileInfo

	err = filepath.WalkDir("/mnt/wdrv", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return nil
		}

		if stat.Uid == uint32(jobUid) {
			relPath, err := filepath.Rel("/mnt/wdrv", path)
			if err != nil {
				return err
			}
			destPath := filepath.Join(projectSlug, relPath)
			destPath = strings.TrimRight(destPath, string(os.PathSeparator))

			uploadList = append(uploadList, uploadFileInfo{
				LocalPath:  path,
				RemotePath: destPath,
			})
		}
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return nil // /mnt/wdrv doesn't exist or is not mounted, skip
		}
		return fmt.Errorf("error walking /mnt/wdrv: %v", err)
	}

	if len(uploadList) == 0 {
		fmt.Fprintln(MultiLogWriter, "No files created by JOB_UID found in /mnt/wdrv to upload.")
		return nil
	}

	filesJSON, err := json.Marshal(uploadList)
	if err != nil {
		return err
	}

	gatewayServer := getenvWithDefault("ACC_JOB_GATEWAY_SERVER", "https://accelerator.iiasa.ac.at")
	casEndpoint := strings.TrimRight(gatewayServer, "/") + "/api/xet-cas"
	accessToken, expiresAt, err := GetAccessToken()
	if err != nil {
		return fmt.Errorf("failed to retrieve access token: %w", err)
	}
	casToken := fmt.Sprintf("xet_session_prj_%s_%s", projectSlug, accessToken)
	registerUrl := strings.TrimRight(gatewayServer, "/") + "/api/xet-cas/v1/cas/bulk-register"

	args := []string{
		"upload",
		projectSlug,
		casEndpoint,
		casToken,
		fmt.Sprintf("%d", expiresAt),
		registerUrl,
		string(filesJSON),
	}

	fmt.Fprintf(MultiLogWriter, "Uploading %d files created by JOB_UID from /mnt/wdrv via hf_xet...\n", len(uploadList))
	ctx, cancel := context.WithTimeout(RootCtx, 30*time.Minute)
	defer cancel()
	if err := RunHelperCommand(ctx, args); err != nil {
		return fmt.Errorf("hf_xet upload failed: %w", err)
	}

	fmt.Fprintf(MultiLogWriter, "Deleting uploaded files from /mnt/wdrv...\n")
	for _, fileInfo := range uploadList {
		if err := os.Remove(fileInfo.LocalPath); err != nil {
			fmt.Fprintf(MultiLogWriter, "Failed to delete %s: %v\n", fileInfo.LocalPath, err)
		} else {
			fmt.Fprintf(MultiLogWriter, "Deleted %s\n", fileInfo.LocalPath)
		}
	}

	return nil
}

