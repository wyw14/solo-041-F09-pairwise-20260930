package sludge

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

// 停泵补偿可能命中的不可恢复情况：自动重试会误停已归属新任务的泵，必须转人工复核。
var (
	ErrPumpNotFound         = errors.New("return pump is not found")
	ErrPumpReadyStale       = errors.New("pump ready confirmation is stale")
	ErrPumpOwnershipChanged = errors.New("pump ownership changed before stop")
)

type Pump struct {
	ID              string           `json:"id"`
	LineID          model.LineID     `json:"line_id"`
	Kind            model.DeviceKind `json:"kind"`
	Running         bool             `json:"running"`
	Primed          bool             `json:"primed"`
	Flow            float64          `json:"flow"`
	OwnerSequence   uuid.UUID        `json:"owner_sequence,omitempty"`
	OwnerGeneration uint64           `json:"owner_generation"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

type PumpController struct {
	mu    sync.RWMutex
	pumps map[string]Pump
}

func NewPumpController(pumps []Pump) (*PumpController, error) {
	if len(pumps) < 2 {
		return nil, errors.New("at least two return pumps are required")
	}
	controller := &PumpController{pumps: make(map[string]Pump, len(pumps))}
	for _, pump := range pumps {
		if pump.ID == "" || pump.LineID == "" {
			return nil, errors.New("pump identity is incomplete")
		}
		controller.pumps[pump.ID] = pump
	}
	return controller, nil
}

func (c *PumpController) Assign(pumpID string, sequenceID uuid.UUID, generation uint64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok || sequenceID == uuid.Nil || generation == 0 {
		return Pump{}, errors.New("pump assignment is invalid")
	}
	pump.OwnerSequence = sequenceID
	pump.OwnerGeneration = generation
	pump.Primed = false
	pump.Running = false
	pump.Flow = 0
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

func (c *PumpController) Ready(pumpID string, sequenceID uuid.UUID, generation uint64, flow float64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		return Pump{}, ErrPumpNotFound
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		return Pump{}, ErrPumpReadyStale
	}
	if flow <= 0 {
		return Pump{}, errors.New("pump ready confirmation has no flow")
	}
	pump.Primed = true
	pump.Running = true
	pump.Flow = flow
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

func (c *PumpController) StopOwned(pumpID string, sequenceID uuid.UUID, generation uint64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		return Pump{}, fmt.Errorf("%w: %s", ErrPumpNotFound, pumpID)
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		return Pump{}, fmt.Errorf("%w: pump %s now owned by sequence %s generation %d (fault owner %s generation %d)",
			ErrPumpOwnershipChanged, pumpID, pump.OwnerSequence, pump.OwnerGeneration, sequenceID, generation)
	}
	pump.Running = false
	pump.Flow = 0
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

// OwnerOf 返回泵当前归属的序列与代际，供补偿执行前判断归属是否已变化。
func (c *PumpController) OwnerOf(pumpID string) (sequenceID uuid.UUID, generation uint64, found bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		return uuid.Nil, 0, false
	}
	return pump.OwnerSequence, pump.OwnerGeneration, true
}

func (c *PumpController) Snapshot() []Pump {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]Pump, 0, len(c.pumps))
	for _, pump := range c.pumps {
		result = append(result, pump)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
