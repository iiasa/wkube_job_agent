package services

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

type StatReport struct {
	Timestamp  time.Time `json:"timestamp"`
	DiskRoot   DiskUsage `json:"disk_root"`
	DiskData   DiskUsage `json:"disk_data"`
	EmptyDir   DiskUsage `json:"empty_dir"`
	AgentStore DiskUsage `json:"agent_store"`
	Memory     MemUsage  `json:"memory"`
	CPUPerCore []float64 `json:"cpu_per_core"`
}

type DiskUsage struct {
	Used        uint64  `json:"used"`
	Total       uint64  `json:"total"`
	UsedPercent float64 `json:"used_percent"`
}

type MemUsage struct {
	Used        uint64  `json:"used"`
	Total       uint64  `json:"total"`
	UsedPercent float64 `json:"used_percent"`
}

func GetStatJson() ([]byte, error) {

	diskRoot, err := disk.Usage("/")
	if err != nil {
		return nil, fmt.Errorf("failed to get root disk usage: %v", err)
	}

	diskData, err := disk.Usage("/mnt/data")
	if err != nil {
		return nil, fmt.Errorf("failed to get /mnt/data usage: %v", err)
	}

	emptyDir, err := disk.Usage("/mnt/sd")
	if err != nil {
		return nil, fmt.Errorf("failed to get /mnt/sd usage: %v", err)
	}

	agentStore, err := disk.Usage("/mnt/agent")
	if err != nil {
		return nil, fmt.Errorf("failed to get /mnt/agent usage: %v", err)
	}

	// RAM usage
	v, err := mem.VirtualMemory()
	if err != nil {
		return nil, fmt.Errorf("failed to get memory stats: %v", err)
	}

	// Per-core CPU usage
	percents, err := cpu.Percent(time.Second, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get CPU usage: %v", err)
	}

	report := StatReport{
		Timestamp: time.Now().UTC(),
		DiskRoot: DiskUsage{
			Used:        diskRoot.Used,
			Total:       diskRoot.Total,
			UsedPercent: diskRoot.UsedPercent,
		},
		DiskData: DiskUsage{
			Used:        diskData.Used,
			Total:       diskData.Total,
			UsedPercent: diskData.UsedPercent,
		},
		EmptyDir: DiskUsage{
			Used:        emptyDir.Used,
			Total:       emptyDir.Total,
			UsedPercent: emptyDir.UsedPercent,
		},
		AgentStore: DiskUsage{
			Used:        agentStore.Used,
			Total:       agentStore.Total,
			UsedPercent: agentStore.UsedPercent,
		},
		Memory: MemUsage{
			Used:        v.Used,
			Total:       v.Total,
			UsedPercent: v.UsedPercent,
		},
		CPUPerCore: percents,
	}

	jsonOutput, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %v", err)
	}

	return jsonOutput, nil
}
