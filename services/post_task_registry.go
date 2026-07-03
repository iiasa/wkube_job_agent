package services

import (
	"encoding/json"
	"fmt"
	"os"
)

func ExecutePostTaskRegistry() error {
	data, err := os.ReadFile(PostWagtTaskRegistry)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read post task registry: %w", err)
	}

	var tasks []IPCRequest
	if err := json.Unmarshal(data, &tasks); err != nil {
		return fmt.Errorf("failed to parse post task registry: %w", err)
	}

	for i, task := range tasks {
		fmt.Fprintf(MultiLogWriter, "executing post task [%d/%d] action=%s\n", i+1, len(tasks), task.Action)
		if err := executePostTaskItem(&task); err != nil {
			fmt.Fprintf(MultiLogWriter, "post task [%d/%d] failed: %v\n", i+1, len(tasks), err)
		}
	}

	return nil
}

func executePostTaskItem(req *IPCRequest) error {
	switch req.Action {
	case "register-validation-with-filename":
		var payload RegisterValidationPayload
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			return fmt.Errorf("invalid payload for %s: %w", req.Action, err)
		}
		_, err := RegisterValidationWithFilename(&payload)
		return err
	default:
		return fmt.Errorf("unknown post task action: %s", req.Action)
	}
}