package sludge

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
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
		return Pump{}, errors.New("return pump is not found")
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		return Pump{}, errors.New("pump ready confirmation is stale")
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
		return Pump{}, errors.New("return pump is not found")
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		return Pump{}, errors.New("pump ownership changed before stop")
	}
	return c.stopLocked(pump, now), nil
}

// Owner 返回泵当前的归属凭证，供补偿任务判断设备是否已被其他序列接管。
func (c *PumpController) Owner(pumpID string) (Pump, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pump, ok := c.pumps[pumpID]
	return pump, ok
}

// StopUnowned 停止当前没有任何归属序列的泵。进程重启后泵归属只存在于内存、
// 会全部丢失，持久化恢复出来的补偿是最后已知归属；此时没有新任务接管，
// 由补偿直接停泵是安全的。若泵已被重新分配则拒绝执行，避免误停新任务。
func (c *PumpController) StopUnowned(pumpID string, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		return Pump{}, errors.New("return pump is not found")
	}
	if pump.OwnerSequence != uuid.Nil {
		return Pump{}, errors.New("pump has a new owner; stop requires manual review")
	}
	return c.stopLocked(pump, now), nil
}

func (c *PumpController) stopLocked(pump Pump, now time.Time) Pump {
	pump.Running = false
	pump.Flow = 0
	pump.UpdatedAt = now.UTC()
	c.pumps[pump.ID] = pump
	return pump
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
