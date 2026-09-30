package sludge

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type SequenceStatus string

const (
	SequenceSwitching SequenceStatus = "switching"
	SequenceRunning   SequenceStatus = "running"
	SequenceFailed    SequenceStatus = "failed"
)

type Sequence struct {
	ID         uuid.UUID      `json:"id"`
	LineID     model.LineID   `json:"line_id"`
	Number     uint64         `json:"number"`
	Generation uint64         `json:"generation"`
	PumpID     string         `json:"pump_id"`
	Status     SequenceStatus `json:"status"`
	StartedAt  time.Time      `json:"started_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type SequenceBook struct {
	mu     sync.RWMutex
	byLine map[model.LineID]Sequence
	pumps  *PumpController
	queue  *journal.CompensationQueue
}

func NewSequenceBook(pumps *PumpController, queue *journal.CompensationQueue) (*SequenceBook, error) {
	if pumps == nil || queue == nil {
		return nil, errors.New("sludge sequence dependencies are required")
	}
	return &SequenceBook{byLine: make(map[model.LineID]Sequence), pumps: pumps, queue: queue}, nil
}

func (b *SequenceBook) Start(lineID model.LineID, pumpID string, number, generation uint64, now time.Time) (Sequence, error) {
	if lineID == "" || pumpID == "" || number == 0 || generation == 0 {
		return Sequence{}, errors.New("sludge sequence is invalid")
	}
	sequence := Sequence{
		ID: uuid.New(), LineID: lineID, Number: number, Generation: generation,
		PumpID: pumpID, Status: SequenceSwitching, StartedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
	if _, err := b.pumps.Assign(pumpID, sequence.ID, generation, now); err != nil {
		return Sequence{}, err
	}
	b.mu.Lock()
	b.byLine[lineID] = sequence
	b.mu.Unlock()
	return sequence, nil
}

func (b *SequenceBook) ConfirmFlow(lineID model.LineID, sequenceID uuid.UUID, generation uint64, flow float64, now time.Time) (Sequence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sequence, ok := b.byLine[lineID]
	if !ok || sequence.ID != sequenceID || sequence.Generation != generation {
		return Sequence{}, errors.New("sludge sequence confirmation is stale")
	}
	if _, err := b.pumps.Ready(sequence.PumpID, sequence.ID, sequence.Generation, flow, now); err != nil {
		return Sequence{}, err
	}
	sequence.Status = SequenceRunning
	sequence.UpdatedAt = now.UTC()
	b.byLine[lineID] = sequence
	return sequence, nil
}

func (b *SequenceBook) Fail(lineID model.LineID, now time.Time) (Sequence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sequence, ok := b.byLine[lineID]
	if !ok {
		return Sequence{}, errors.New("sludge sequence is not active")
	}
	sequence.Status = SequenceFailed
	sequence.UpdatedAt = now.UTC()
	b.byLine[lineID] = sequence
	compensation, err := journal.NewCompensation(lineID, sequence.ID, sequence.Generation, sequence.PumpID, "stop", now.Add(time.Second))
	if err != nil {
		return Sequence{}, err
	}
	// 同一故障（序列+代际+泵+动作）已存在停泵任务时不叠加：
	// 无论旧任务是等待重试还是已转人工复核，都不自动再排一条。
	if _, err := b.queue.Schedule(compensation); err != nil && !errors.Is(err, journal.ErrCompensationDuplicate) {
		return Sequence{}, err
	}
	return sequence, nil
}

func (b *SequenceBook) ExecuteCompensation(ctx context.Context, compensation journal.Compensation, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if compensation.Action != "stop" {
		return journal.NewManualReviewError("unsupported sludge compensation action: " + compensation.Action)
	}
	_, err := b.pumps.StopOwned(compensation.DeviceID, compensation.SequenceID, compensation.Generation, now)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrPumpOwnershipChanged):
		// 泵已被新序列接管：自动停泵会误停新任务，转人工复核。
		return journal.NewManualReviewError(err.Error())
	case errors.Is(err, ErrPumpNotFound):
		// 设备已不在册同样不可自动恢复，转人工复核而非反复重试。
		return journal.NewManualReviewError(err.Error())
	default:
		return err
	}
}

// RecoverPending 在进程重启后依据持久化的待执行停泵任务重建内存状态：
// 每台设备上最新故障（创建时间/代际）的任务取得泵归属，使补偿可继续执行；
// 同设备更旧的任务意味着归属已转移给后续序列，直接转人工复核，避免误停新任务。
func (b *SequenceBook) RecoverPending(items []journal.Compensation) error {
	byDevice := make(map[string][]journal.Compensation)
	for _, item := range items {
		if item.Status != journal.CompensationPending || item.Action != "stop" {
			continue
		}
		byDevice[item.DeviceID] = append(byDevice[item.DeviceID], item)
	}
	for deviceID, deviceItems := range byDevice {
		sort.SliceStable(deviceItems, func(i, j int) bool {
			if deviceItems[i].CreatedAt.Equal(deviceItems[j].CreatedAt) {
				return deviceItems[i].Generation > deviceItems[j].Generation
			}
			return deviceItems[i].CreatedAt.After(deviceItems[j].CreatedAt)
		})
		winner := deviceItems[0]
		if _, err := b.pumps.Assign(deviceID, winner.SequenceID, winner.Generation, winner.CreatedAt); err != nil {
			return err
		}
		for _, stale := range deviceItems[1:] {
			reason := fmt.Sprintf("pump %s ownership moved to sequence %s before restart", deviceID, winner.SequenceID)
			if err := b.queue.MarkReview(stale, reason); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *SequenceBook) Current(lineID model.LineID) (Sequence, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sequence, ok := b.byLine[lineID]
	return sequence, ok
}
