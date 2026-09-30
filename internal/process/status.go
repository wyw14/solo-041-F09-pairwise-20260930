package process

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/aeration"
	"github.com/wyw14/cry-97/internal/discharge"
	"github.com/wyw14/cry-97/internal/dosing"
	"github.com/wyw14/cry-97/internal/intake"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/settling"
	"github.com/wyw14/cry-97/internal/sludge"
)

type LineStatus struct {
	Line          model.LineState        `json:"line"`
	Batch         *model.Batch           `json:"batch,omitempty"`
	Intake        *intake.Admission      `json:"intake,omitempty"`
	Equalizers    []intake.Equalizer     `json:"equalizers"`
	Dose          *dosing.Change         `json:"dose,omitempty"`
	LabResult     *model.LabResult       `json:"lab_result,omitempty"`
	Permit        *discharge.Permit      `json:"permit,omitempty"`
	Cycle         *settling.Cycle        `json:"cycle,omitempty"`
	Handover      *sludge.Handover       `json:"handover,omitempty"`
	Sequence      *sludge.Sequence       `json:"sequence,omitempty"`
	Settling      settling.Snapshot      `json:"settling"`
	Samples       []model.Sample         `json:"samples"`
	SampleCursor  uint64                 `json:"sample_cursor"`
	Compensations []journal.Compensation `json:"compensations"`
	Pumps         []sludge.Pump          `json:"pumps"`
	Blowers       []aeration.Blower      `json:"blowers"`
	Valves        []discharge.Valve      `json:"valves"`
	Commands      []model.DeviceCommand  `json:"commands"`
	AlarmCount    int                    `json:"alarm_count"`
	ObservedAt    time.Time              `json:"observed_at"`
}

func (p *Plant) Status(lineID model.LineID) (LineStatus, error) {
	p.mu.RLock()
	line, ok := p.lines[lineID]
	p.mu.RUnlock()
	var batch *model.Batch
	var dose *dosing.Change
	var labResult *model.LabResult
	var permit *discharge.Permit
	if value, hasBatch := p.ActiveBatch(lineID); hasBatch {
		batchID := value.ID
		batch = &value
		if doseValue, found := p.dosing.Latest(batchID); found {
			dose = &doseValue
		}
		if value.QualificationSample != uuid.Nil {
			if resultValue, found := p.labResults.Get(value.QualificationSample); found {
				labResult = &resultValue
			}
		}
		if permitValue, found := p.permits.Current(batchID); found {
			permit = &permitValue
		}
	}
	if !ok {
		return LineStatus{}, errors.New("process line is not found")
	}
	settlingState, ok := p.settling[lineID]
	if !ok {
		return LineStatus{}, errors.New("settling state is not found")
	}
	var admission *intake.Admission
	if value, found := p.intake.Current(lineID); found {
		admission = &value
	}
	var cycle *settling.Cycle
	if value, found := p.cycles.Current(lineID); found {
		cycle = &value
	}
	var handover *sludge.Handover
	if value, found := p.handovers.Current(lineID); found {
		handover = &value
	}
	var sequence *sludge.Sequence
	if value, found := p.sequences.Current(lineID); found {
		sequence = &value
	}
	return LineStatus{
		Line: line, Batch: batch, Intake: admission, Equalizers: p.equalizers.Snapshot(), Dose: dose, LabResult: labResult, Permit: permit,
		Cycle: cycle, Handover: handover, Sequence: sequence, Settling: settlingState.Snapshot(),
		Samples: p.sampler.LineSnapshot(lineID), SampleCursor: p.receiver.Current(lineID),
		Compensations: filterCompensations(p.compensations.List(), lineID),
		Pumps:         filterPumps(p.pumps.Snapshot(), lineID), Blowers: p.aeration.Blowers(),
		Valves: p.valves.Snapshot(), Commands: p.devices.All(),
		AlarmCount: len(p.alarms.Active(lineID)), ObservedAt: p.now().UTC(),
	}, nil
}

func filterCompensations(values []journal.Compensation, lineID model.LineID) []journal.Compensation {
	result := make([]journal.Compensation, 0, len(values))
	for _, value := range values {
		if value.LineID == lineID {
			result = append(result, value)
		}
	}
	return result
}

func filterPumps(values []sludge.Pump, lineID model.LineID) []sludge.Pump {
	result := make([]sludge.Pump, 0, len(values))
	for _, value := range values {
		if value.LineID == lineID {
			result = append(result, value)
		}
	}
	return result
}

func (p *Plant) Healthy() bool {
	return len(p.Lines()) > 0 && p.store != nil && p.devices != nil
}
