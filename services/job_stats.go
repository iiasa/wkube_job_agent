package services

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type StatReport struct {
	Timestamp        time.Time `json:"timestamp"`
	WorkingDir       DiskUsage `json:"working_dir"`
	Memory           MemUsage  `json:"memory"`
	CPUPercent       float64   `json:"cpu_percent"`
	CPUThrottledUsec uint64    `json:"cpu_throttled_usec"`
}

type DiskUsage struct {
	UsedBytes uint64 `json:"used_bytes"`
}

type MemUsage struct {
	Used        uint64  `json:"used"`
	Total       uint64  `json:"total"`
	UsedPercent float64 `json:"used_percent"`
}

// Detects if system uses cgroup v2
func isCgroupV2() bool {
	_, err := os.Stat("/sys/fs/cgroup/cgroup.controllers")
	return err == nil
}

func getMemoryUsageV2() (used uint64, limit uint64, usedPercent float64, err error) {
	if !isCgroupV2() {
		err = fmt.Errorf("cgroup v1 not implemented for memory stats")
		return
	}

	usedBytes, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		return
	}
	limitBytes, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return
	}

	used, _ = strconv.ParseUint(strings.TrimSpace(string(usedBytes)), 10, 64)

	limitStr := strings.TrimSpace(string(limitBytes))
	if limitStr == "max" {
		limit = used
		usedPercent = 100.0
	} else {
		limit, _ = strconv.ParseUint(limitStr, 10, 64)
		usedPercent = float64(used) / float64(limit) * 100
	}

	return
}

func readCPUStats() (usage, throttled uint64, err error) {
	if !isCgroupV2() {
		err = fmt.Errorf("cgroup v1 not implemented for CPU stats")
		return
	}

	data, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "usage_usec") {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				usage, _ = strconv.ParseUint(fields[1], 10, 64)
			}
		} else if strings.HasPrefix(line, "throttled_usec") {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				throttled, _ = strconv.ParseUint(fields[1], 10, 64)
			}
		}
	}
	return
}

func getCPUPercentAndThrottle(duration time.Duration) (percent float64, throttledDelta uint64, err error) {
	startUsage, startThrottled, err := readCPUStats()
	if err != nil {
		return
	}
	time.Sleep(duration)
	endUsage, endThrottled, err := readCPUStats()
	if err != nil {
		return
	}

	deltaUsage := float64(endUsage-startUsage) / 1_000_000.0 // microseconds → seconds
	throttledDelta = endThrottled - startThrottled
	percent = (deltaUsage / duration.Seconds()) * 100.0
	return
}

func getWorkingDirDiskUsage() (uint64, error) {
	cmd := exec.Command("du", "-sb", ".")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to execute du: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 1 {
		return 0, fmt.Errorf("unexpected du output: %s", output)
	}
	return strconv.ParseUint(fields[0], 10, 64)
}

func GetStatJson() ([]byte, error) {
	if !isCgroupV2() {
		return nil, fmt.Errorf("cgroup v1 is not implemented for stats collection")
	}

	usedBytes, err := getWorkingDirDiskUsage()
	if err != nil {
		return nil, fmt.Errorf("failed to get working dir disk usage: %v", err)
	}

	memUsed, memLimit, memPercent, err := getMemoryUsageV2()
	if err != nil {
		return nil, err
	}

	cpuPercent, throttledUsec, err := getCPUPercentAndThrottle(time.Second)
	if err != nil {
		return nil, err
	}

	report := StatReport{
		Timestamp: time.Now().UTC(),
		WorkingDir: DiskUsage{
			UsedBytes: usedBytes,
		},
		Memory: MemUsage{
			Used:        memUsed,
			Total:       memLimit,
			UsedPercent: memPercent,
		},
		CPUPercent:       cpuPercent,
		CPUThrottledUsec: throttledUsec,
	}

	return json.Marshal(report)
}
