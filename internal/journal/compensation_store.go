package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// CompensationStore 持久化待执行/待复核的补偿任务，保证进程重启后队列不丢失。
type CompensationStore struct {
	path string
	mu   sync.Mutex
}

func NewCompensationStore(path string) (*CompensationStore, error) {
	if path == "" {
		return nil, errors.New("compensation store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create compensation directory: %w", err)
	}
	return &CompensationStore{path: path}, nil
}

// Load 读取全部补偿记录，按下次执行时间排序。文件不存在时返回空切片。
func (s *CompensationStore) Load() ([]Compensation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return []Compensation{}, nil
	}
	if err != nil {
		return nil, err
	}
	var records []Compensation
	if len(data) > 0 {
		if err := json.Unmarshal(data, &records); err != nil {
			return nil, fmt.Errorf("decode compensation store: %w", err)
		}
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].DueAt.Equal(records[j].DueAt) {
			return records[i].ID.String() < records[j].ID.String()
		}
		return records[i].DueAt.Before(records[j].DueAt)
	})
	return records, nil
}

// Save 原子覆写全部补偿记录（临时文件 + rename），避免崩溃产生半截文件。
func (s *CompensationStore) Save(records []Compensation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if records == nil {
		records = []Compensation{}
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	temporary := s.path + ".next"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o640); err != nil {
		return err
	}
	return os.Rename(temporary, s.path)
}
