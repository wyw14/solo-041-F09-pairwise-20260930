package sludge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

func newTestBook(t *testing.T) (*SequenceBook, *journal.CompensationQueue, *PumpController) {
	t.Helper()
	pumps, err := NewPumpController([]Pump{
		{ID: "pump-line-1-A", LineID: "line-1", Kind: model.DevicePump},
		{ID: "pump-line-1-B", LineID: "line-1", Kind: model.DevicePump},
	})
	if err != nil {
		t.Fatal(err)
	}
	queue := journal.NewCompensationQueue()
	book, err := NewSequenceBook(pumps, queue)
	if err != nil {
		t.Fatal(err)
	}
	return book, queue, pumps
}

func TestFailDoesNotStackDuplicateStopTasks(t *testing.T) {
	book, queue, _ := newTestBook(t)
	now := time.Now().UTC()
	if _, err := book.Start("line-1", "pump-line-1-A", 1, 7, now); err != nil {
		t.Fatal(err)
	}
	if _, err := book.Fail("line-1", now); err != nil {
		t.Fatal(err)
	}
	// 同一故障重复上报（补偿尚未执行）不得叠加第二条相同停泵任务。
	if _, err := book.Fail("line-1", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if pending := queue.Pending(); len(pending) != 1 {
		t.Fatalf("expected exactly one stop task, got %d", len(pending))
	}
}

func TestStopCompensationSucceedsForCurrentOwner(t *testing.T) {
	book, queue, pumps := newTestBook(t)
	now := time.Now().UTC()
	sequence, err := book.Start("line-1", "pump-line-1-A", 1, 7, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pumps.Ready(sequence.PumpID, sequence.ID, sequence.Generation, 120, now); err != nil {
		t.Fatal(err)
	}
	if _, err := book.Fail("line-1", now); err != nil {
		t.Fatal(err)
	}
	pending := queue.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected scheduled compensation")
	}
	if err := book.ExecuteCompensation(context.Background(), pending[0], now.Add(2*time.Second)); err != nil {
		t.Fatalf("stop owned pump: %v", err)
	}
}

func TestStopCompensationAfterOwnershipChangeRequiresReview(t *testing.T) {
	book, queue, _ := newTestBook(t)
	now := time.Now().UTC()
	first, err := book.Start("line-1", "pump-line-1-A", 1, 7, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := book.Fail("line-1", now); err != nil {
		t.Fatal(err)
	}

	// 泵在补偿执行前被新序列接管（设备归属已经变化）。
	if _, err := book.Start("line-1", "pump-line-1-A", 2, 8, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	var oldTask journal.Compensation
	for _, item := range queue.Pending() {
		if item.SequenceID == first.ID {
			oldTask = item
		}
	}
	if oldTask.ID == uuid.Nil {
		t.Fatal("original fault compensation missing")
	}
	// 旧故障的停泵绝不能停掉新归属的泵，应直接转人工复核。
	if err := book.ExecuteCompensation(context.Background(), oldTask, now.Add(2*time.Minute)); !errors.Is(err, journal.ErrManualReviewRequired) {
		t.Fatalf("expected manual review, got %v", err)
	}
}

func TestRecoverPendingRestoresOwnershipAndFlagsStaleTasks(t *testing.T) {
	book, queue, pumps := newTestBook(t)
	base := time.Now().UTC()

	oldItem, err := journal.NewCompensation("line-1", uuid.New(), 1, "pump-line-1-A", "stop", base)
	if err != nil {
		t.Fatal(err)
	}
	newItem, err := journal.NewCompensation("line-1", uuid.New(), 2, "pump-line-1-A", "stop", base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Schedule(oldItem); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Schedule(newItem); err != nil {
		t.Fatal(err)
	}

	if err := book.RecoverPending(queue.Pending()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// 最新故障取得泵归属，补偿重启后可继续停泵。
	owner, generation, found := pumps.OwnerOf("pump-line-1-A")
	if !found || owner != newItem.SequenceID || generation != 2 {
		t.Fatalf("ownership not restored to latest fault: %s gen %d", owner, generation)
	}
	if err := book.ExecuteCompensation(context.Background(), newItem, base.Add(2*time.Minute)); err != nil {
		t.Fatalf("latest fault compensation should execute after recovery: %v", err)
	}

	// 旧任务被标记人工复核而不是去停新归属的泵。
	var stale journal.Compensation
	for _, item := range queue.List() {
		if item.ID == oldItem.ID {
			stale = item
		}
	}
	if stale.Status != journal.CompensationReview {
		t.Fatalf("expected stale task manual_review, got %q", stale.Status)
	}
	if stale.LastError == "" {
		t.Fatal("stale task must record review reason")
	}
}
