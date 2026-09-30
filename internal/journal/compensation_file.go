package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type compensationFile struct {
	Version int            `json:"version"`
	Items   []Compensation `json:"items"`
}

// persistLocked 将当前任务快照原子写入磁盘。调用方必须持有 q.mu。
func (q *CompensationQueue) persistLocked() error {
	if q.path == "" {
		return nil
	}
	items := make([]Compensation, 0, len(q.items))
	for _, item := range q.items {
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	data, err := json.MarshalIndent(compensationFile{Version: 1, Items: items}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode compensations: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(q.path), 0o755); err != nil {
		return err
	}
	temporary := q.path + ".next"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o640); err != nil {
		return fmt.Errorf("write compensations: %w", err)
	}
	if err := os.Rename(temporary, q.path); err != nil {
		return fmt.Errorf("commit compensations: %w", err)
	}
	return nil
}

// load 从磁盘恢复未完成的补偿任务；已成功的任务不会出现在文件中。
func (q *CompensationQueue) load() error {
	data, err := os.ReadFile(q.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read compensations: %w", err)
	}
	var file compensationFile
	if err := json.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("decode compensations: %w", err)
	}
	for _, item := range file.Items {
		item.Status = normalizeStatus(item.Status)
		if item.MaxAttempts == 0 {
			item.MaxAttempts = q.maxAttempts
		}
		q.items[item.FaultKey()] = item
	}
	return nil
}

func normalizeStatus(status CompensationStatus) CompensationStatus {
	switch status {
	case CompensationPending, CompensationReview:
		return status
	default:
		// 旧版本/未知状态一律按待执行处理，由执行循环重新判定，避免漏停泵。
		return CompensationPending
	}
}
