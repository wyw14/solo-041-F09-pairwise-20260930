package process

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/aeration"
	"github.com/wyw14/cry-97/internal/alarm"
	"github.com/wyw14/cry-97/internal/discharge"
	"github.com/wyw14/cry-97/internal/dosing"
	"github.com/wyw14/cry-97/internal/intake"
	"github.com/wyw14/cry-97/internal/interlock"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/lab"
	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/sensor"
	"github.com/wyw14/cry-97/internal/settling"
	"github.com/wyw14/cry-97/internal/sludge"
)

type Plant struct {
	mu            sync.RWMutex
	now           func() time.Time
	lines         map[model.LineID]model.LineState
	batches       map[uuid.UUID]model.Batch
	lineBatches   map[model.LineID]uuid.UUID
	store         *journal.FileStore
	snapshots     *journal.SnapshotStore
	intake        *intake.Controller
	equalizers    *intake.EqualizationPool
	aeration      *aeration.Controller
	dosing        *dosing.Service
	labResults    *lab.Results
	permits       *discharge.PermitBook
	valves        *discharge.ValveController
	alarms        *alarm.Service
	settling      map[model.LineID]*settling.State
	cycles        *settling.CycleBook
	interlocks    *interlock.Arbiter
	drains        *sludge.DrainService
	pumps         *sludge.PumpController
	handovers     *sludge.HandoverBook
	sequences     *sludge.SequenceBook
	compensations *journal.CompensationQueue
	devices       *DeviceBus
	sampler       *sensor.Sampler
	receiver      *sensor.Receiver
}

func NewPlant(dataDir string, lines []model.LineID, now func() time.Time) (*Plant, error) {
	if dataDir == "" || len(lines) == 0 || now == nil {
		return nil, errors.New("plant data directory, lines and clock are required")
	}
	store, err := journal.NewFileStore(filepath.Join(dataDir, "journal"))
	if err != nil {
		return nil, err
	}
	snapshots, err := journal.NewSnapshotStore(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		return nil, err
	}
	cursor, err := journal.NewCursorStore(filepath.Join(dataDir, "sample-cursors.json"))
	if err != nil {
		return nil, err
	}
	equalizers := make([]intake.Equalizer, 0, len(lines))
	blowers := make([]aeration.Blower, 0, len(lines))
	pumpList := make([]sludge.Pump, 0, len(lines)*2)
	for index, lineID := range lines {
		equalizers = append(equalizers, intake.Equalizer{ID: "EQ-" + string(lineID), Capacity: 1600 + float64(index*150)})
		blowers = append(blowers, aeration.Blower{ID: "blower-basin-" + string(lineID), BasinID: "basin-" + string(lineID)})
		pumpList = append(pumpList,
			sludge.Pump{ID: "pump-" + string(lineID) + "-A", LineID: lineID, Kind: model.DevicePump},
			sludge.Pump{ID: "pump-" + string(lineID) + "-B", LineID: lineID, Kind: model.DevicePump},
		)
	}
	pool, err := intake.NewEqualizationPool(equalizers)
	if err != nil {
		return nil, err
	}
	intakeController, err := intake.NewController(pool)
	if err != nil {
		return nil, err
	}
	for _, lineID := range lines {
		if err := intakeController.Configure(lineID, 1800); err != nil {
			return nil, err
		}
	}
	decoder, err := sensor.NewBatchDecoder(24)
	if err != nil {
		return nil, err
	}
	windows, err := aeration.NewWindowBook(10)
	if err != nil {
		return nil, err
	}
	fleet, err := aeration.NewBlowerFleet(blowers)
	if err != nil {
		return nil, err
	}
	aerationController, err := aeration.NewController(decoder, windows, fleet)
	if err != nil {
		return nil, err
	}
	valves, err := discharge.NewValveController(lines)
	if err != nil {
		return nil, err
	}
	permits, err := discharge.NewPermitBook(valves)
	if err != nil {
		return nil, err
	}
	pumps, err := sludge.NewPumpController(pumpList)
	if err != nil {
		return nil, err
	}
	queue, err := journal.NewPersistedCompensationQueue(filepath.Join(dataDir, "compensations"))
	if err != nil {
		return nil, err
	}
	sequences, err := sludge.NewSequenceBook(pumps, queue)
	if err != nil {
		return nil, err
	}
	// 进程重启后泵归属与序列状态只存在于内存：依据未完成的停泵补偿重建归属，
	// 使补偿能继续执行而不是被误判为"归属已变化"。同一设备上较旧的待执行任务
	// 说明归属在崩溃前已转给新序列，恢复时直接转人工复核，避免误停新任务。
	if err := sequences.RecoverPending(queue.Pending()); err != nil {
		return nil, err
	}
	arbiter := interlock.NewArbiter()
	drains, err := sludge.NewDrainService(arbiter)
	if err != nil {
		return nil, err
	}
	devices := NewDeviceBus()
	dosingService, err := dosing.NewService(store, devices, dosing.NewLedger())
	if err != nil {
		return nil, err
	}
	receiver, err := sensor.NewReceiver(store, cursor)
	if err != nil {
		return nil, err
	}
	sampler := sensor.NewSampler()
	plant := &Plant{
		now: now, lines: make(map[model.LineID]model.LineState), batches: make(map[uuid.UUID]model.Batch),
		lineBatches: make(map[model.LineID]uuid.UUID), store: store, snapshots: snapshots,
		intake: intakeController, equalizers: pool, aeration: aerationController, dosing: dosingService,
		labResults: lab.NewResults(), permits: permits, valves: valves, alarms: alarm.NewService(),
		settling: make(map[model.LineID]*settling.State), cycles: settling.NewCycleBook(),
		interlocks: arbiter, drains: drains, pumps: pumps, handovers: sludge.NewHandoverBook(),
		sequences: sequences, compensations: queue, devices: devices, sampler: sampler, receiver: receiver,
	}
	for _, lineID := range lines {
		plant.lines[lineID] = model.NewLineState(lineID, now())
		listener := alarm.NewBlanketListener(plant.alarms, now)
		state, stateErr := settling.NewState(lineID, "settler-"+string(lineID), listener)
		if stateErr != nil {
			return nil, stateErr
		}
		listener.Bind(state)
		plant.settling[lineID] = state
	}
	if err := sampler.Subscribe(func(sample model.Sample) {
		if sample.Kind == model.SampleSludgeBlanket {
			_, _ = plant.UpdateBlanket(sample.LineID, sample.Value)
		}
	}); err != nil {
		return nil, err
	}
	return plant, nil
}

func (p *Plant) StartBatch(ctx context.Context, lineID model.LineID, flow float64, source string) (model.Batch, error) {
	now := p.now()
	if _, err := p.intake.Admit(ctx, lineID, flow, source, now); err != nil {
		return model.Batch{}, err
	}
	p.mu.Lock()
	if existingID, ok := p.lineBatches[lineID]; ok && p.batches[existingID].IsActive() {
		p.mu.Unlock()
		return model.Batch{}, errors.New("process line already has an active batch")
	}
	batch := model.NewBatch(lineID, now)
	p.batches[batch.ID] = batch
	p.lineBatches[lineID] = batch.ID
	state := p.lines[lineID]
	state.BatchID = batch.ID.String()
	state.UpdatedAt = now.UTC()
	p.lines[lineID] = state
	p.mu.Unlock()
	event, err := model.NewEvent(lineID, batch.Generation, model.EventBatchCreated, batch, now)
	if err != nil {
		return model.Batch{}, err
	}
	if _, err := p.store.Append(ctx, event); err != nil {
		return model.Batch{}, err
	}
	return batch, nil
}

func (p *Plant) AdvanceBatch(ctx context.Context, lineID model.LineID, next model.ProcessStage) (model.Batch, error) {
	batch, ok := p.ActiveBatch(lineID)
	if !ok {
		return model.Batch{}, errors.New("process line has no active batch")
	}
	now := p.now()
	updated, err := batch.Move(next, now)
	if err != nil {
		return model.Batch{}, err
	}
	event, err := model.NewEvent(lineID, updated.Generation, model.EventStageChanged, updated, now)
	if err != nil {
		return model.Batch{}, err
	}
	if _, err := p.store.Append(ctx, event); err != nil {
		return model.Batch{}, err
	}
	p.mu.Lock()
	p.batches[updated.ID] = updated
	state := p.lines[lineID]
	state.Stage = updated.Stage
	state.UpdatedAt = now.UTC()
	p.lines[lineID] = state
	p.mu.Unlock()
	return updated, nil
}

func (p *Plant) Lines() []model.LineState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]model.LineState, 0, len(p.lines))
	for _, state := range p.lines {
		result = append(result, state.Clone())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (p *Plant) ActiveBatch(lineID model.LineID) (model.Batch, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	id, ok := p.lineBatches[lineID]
	if !ok {
		return model.Batch{}, false
	}
	batch, ok := p.batches[id]
	return batch, ok
}
