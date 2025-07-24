package services

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// You must have this defined somewhere in your package:
// var MultiLogWriter io.Writer

func bytesToGB(b uint64) float64 {
	return float64(b) / (1024 * 1024 * 1024)
}

func VerboseResourceReport() error {
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err != nil {
		return fmt.Errorf("cgroup v1 is not supported")
	}

	// ------------------ CPU Stats -------------------
	cpuData, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return fmt.Errorf("failed to read cpu.stat: %v", err)
	}

	var usage, user, system, throttled, periods, throttledPeriods uint64

	for _, line := range strings.Split(string(cpuData), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		val, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "usage_usec":
			usage = val
		case "user_usec":
			user = val
		case "system_usec":
			system = val
		case "throttled_usec":
			throttled = val
		case "nr_periods":
			periods = val
		case "nr_throttled":
			throttledPeriods = val
		}
	}

	cpuMaxData, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return fmt.Errorf("failed to read cpu.max: %v", err)
	}
	cpuParts := strings.Fields(string(cpuMaxData))
	var quota, period uint64
	if cpuParts[0] != "max" {
		quota, _ = strconv.ParseUint(cpuParts[0], 10, 64)
	}
	period, _ = strconv.ParseUint(cpuParts[1], 10, 64)

	throttleRatio := float64(throttledPeriods) / float64(periods) * 100
	throttleTimeSec := float64(throttled) / 1e6
	cpuUsageSec := float64(usage) / 1e6

	var quotaCores, allowedCPUTime, efficiency float64
	if quota > 0 && period > 0 {
		quotaCores = float64(quota) / float64(period)
		allowedCPUTime = float64(periods*quota) / 1e6
		efficiency = cpuUsageSec / allowedCPUTime * 100
	}

	// ------------------ Memory Stats -------------------
	readUint := func(path string) (uint64, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return 0, err
		}
		str := strings.TrimSpace(string(data))
		if str == "max" {
			return 0, nil
		}
		return strconv.ParseUint(str, 10, 64)
	}

	memCurrent, err := readUint("/sys/fs/cgroup/memory.current")
	if err != nil {
		return fmt.Errorf("failed to read memory.current: %v", err)
	}
	memPeak, err := readUint("/sys/fs/cgroup/memory.peak")
	if err != nil {
		return fmt.Errorf("failed to read memory.peak: %v", err)
	}
	memMaxRaw, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return fmt.Errorf("failed to read memory.max: %v", err)
	}
	memMaxStr := strings.TrimSpace(string(memMaxRaw))
	var memLimit uint64
	var memPercent float64
	if memMaxStr == "max" {
		memLimit = 0
		memPercent = 0
	} else {
		memLimit, _ = strconv.ParseUint(memMaxStr, 10, 64)
		memPercent = float64(memCurrent) / float64(memLimit) * 100
	}

	// ------------------ Disk Usage -------------------
	cmd := exec.Command("du", "-sb", ".")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to execute du: %v", err)
	}
	duFields := strings.Fields(string(output))
	diskUsageBytes, _ := strconv.ParseUint(duFields[0], 10, 64)

	// ------------------ Print Report -------------------
	w := MultiLogWriter
	fmt.Fprintf(w, "\n📊 Resource Usage Report:\n")

	fmt.Fprintf(w, "\n🧠 Memory:\n")
	fmt.Fprintf(w, "- Current usage:             %.2f GB\n", bytesToGB(memCurrent))
	fmt.Fprintf(w, "- Peak usage:                %.2f GB\n", bytesToGB(memPeak))
	if memLimit > 0 {
		fmt.Fprintf(w, "- Memory limit:              %.2f GB\n", bytesToGB(memLimit))
		fmt.Fprintf(w, "- Memory usage:              %.2f%% of limit\n", memPercent)
	} else {
		fmt.Fprintf(w, "- Memory limit:              unlimited\n")
	}

	fmt.Fprintf(w, "\n🖥️  CPU:\n")
	fmt.Fprintf(w, "- Total CPU time used:       %.3f sec\n", cpuUsageSec)
	fmt.Fprintf(w, "- User mode time:            %.3f sec\n", float64(user)/1e6)
	fmt.Fprintf(w, "- System mode time:          %.3f sec\n", float64(system)/1e6)
	fmt.Fprintf(w, "- Quota enforcement periods: %d\n", periods)
	fmt.Fprintf(w, "- Throttled periods:         %d (%.2f%%)\n", throttledPeriods, throttleRatio)
	fmt.Fprintf(w, "- Throttled time:            %.3f sec\n", throttleTimeSec)

	if quota > 0 {
		fmt.Fprintf(w, "- CPU quota:                 %.2f core(s) per period\n", quotaCores)
		fmt.Fprintf(w, "- Allowed CPU time:          %.3f sec\n", allowedCPUTime)
		fmt.Fprintf(w, "- CPU efficiency:            %.2f%%\n", efficiency)
	} else {
		fmt.Fprintf(w, "- CPU quota:                 unlimited (no throttling expected)\n")
	}

	fmt.Fprintf(w, "\n📂 Working Directory:\n")
	fmt.Fprintf(w, "- Disk usage:                %.2f GB\n", bytesToGB(diskUsageBytes))

	return nil
}
