package process

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/sludge"
)

func (p *Plant) StartHandover(lineID model.LineID, fromPump, toPump string) (sludge.Handover, error) {
	p.mu.RLock()
	state, ok := p.lines[lineID]
	p.mu.RUnlock()
	if !ok || state.Emergency {
		return sludge.Handover{}, errors.New("process line cannot start pump handover")
	}
	return p.handovers.Start(lineID, fromPump, toPump, state.Generation, p.now())
}

func (p *Plant) ConfirmHandover(lineID model.LineID, pumpID string, generation uint64, flow float64) (sludge.Handover, error) {
	handover, err := p.handovers.ConfirmReady(lineID, pumpID, generation, flow, p.now())
	if err != nil {
		return sludge.Handover{}, err
	}
	p.mu.Lock()
	state := p.lines[lineID]
	state.UpdatedAt = p.now().UTC()
	p.lines[lineID] = state
	p.mu.Unlock()
	return handover, nil
}

func (p *Plant) StartSludgeSequence(lineID model.LineID, pumpID string, number uint64) (sludge.Sequence, error) {
	p.mu.RLock()
	state, ok := p.lines[lineID]
	p.mu.RUnlock()
	if !ok || state.Emergency {
		return sludge.Sequence{}, errors.New("process line cannot start sludge sequence")
	}
	return p.sequences.Start(lineID, pumpID, number, state.Generation, p.now())
}

func (p *Plant) ConfirmSludgeFlow(lineID model.LineID, sequenceID uuid.UUID, generation uint64, flow float64) (sludge.Sequence, error) {
	return p.sequences.ConfirmFlow(lineID, sequenceID, generation, flow, p.now())
}

func (p *Plant) FailSludgeSequence(lineID model.LineID) (sludge.Sequence, error) {
	return p.sequences.Fail(lineID, p.now())
}

func (p *Plant) RunCompensations(ctx context.Context) (int, error) {
	now := p.now()
	return p.compensations.ExecuteDue(ctx, now, func(callCtx context.Context, item journal.Compensation) error {
		return p.sequences.ExecuteCompensation(callCtx, item, now)
	})
}

func (p *Plant) Pumps() []sludge.Pump {
	return p.pumps.Snapshot()
}

// Compensations 返回全部停泵补偿任务（待执行/退避重试/人工复核），供状态页展示。
func (p *Plant) Compensations() []journal.Compensation {
	return p.compensations.List()
}

// ResolveCompensationReview 人工确认现场已处置后移除复核任务。
func (p *Plant) ResolveCompensationReview(id uuid.UUID) error {
	resolved, err := p.compensations.ResolveReview(id)
	if err != nil {
		return err
	}
	if !resolved {
		return errors.New("compensation review task is not found")
	}
	return nil
}
