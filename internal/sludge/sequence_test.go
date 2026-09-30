package sludge_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/sludge"
)

const (
	lineA = model.LineID("line-1")
	pumpA = "pump-line-1-A"
	pumpB = "pump-line-1-B"
)

type testHarness struct {
	book      *sludge.SequenceBook
	pumps     *sludge.PumpController
	queue     *journal.CompensationQueue
	storePath string
}

func newTestHarness(t *testing.T) testHarness {
	t.Helper()
	controller, err := sludge.NewPumpController([]sludge.Pump{
		{ID: pumpA, LineID: lineA, Kind: model.DevicePump},
		{ID: pumpB, LineID: lineA, Kind: model.DevicePump},
	})
	if err != nil {
		t.Fatal(err)
	}
	storePath := filepath.Join(t.TempDir(), "compensations.json")
	store, err := journal.NewCompensationStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	book, err := sludge.NewSequenceBook(controller, queue)
	if err != nil {
		t.Fatal(err)
	}
	return testHarness{book: book, pumps: controller, queue: queue, storePath: storePath}
}

// reopen 模拟进程重启：新泵控制器（内存归属全空），队列从同一份持久化文件恢复。
func (h testHarness) reopen(t *testing.T, runningPumps ...sludge.Pump) (*sludge.SequenceBook, *sludge.PumpController, *journal.CompensationQueue) {
	t.Helper()
	if len(runningPumps) == 0 {
		runningPumps = []sludge.Pump{
			{ID: pumpA, LineID: lineA, Kind: model.DevicePump},
			{ID: pumpB, LineID: lineA, Kind: model.DevicePump},
		}
	}
	controller, err := sludge.NewPumpController(runningPumps)
	if err != nil {
		t.Fatal(err)
	}
	store, err := journal.NewCompensationStore(h.storePath)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	book, err := sludge.NewSequenceBook(controller, queue)
	if err != nil {
		t.Fatal(err)
	}
	return book, controller, queue
}

func execute(t *testing.T, queue *journal.CompensationQueue, book *sludge.SequenceBook, now time.Time) (int, error) {
	t.Helper()
	return queue.ExecuteDue(context.Background(), now, func(ctx context.Context, item journal.Compensation) error {
		return book.ExecuteCompensation(ctx, item, now)
	})
}

func TestFailedSequenceSchedulesSingleDelayedStop(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	h := newTestHarness(t)
	if _, err := h.book.Start(lineA, pumpA, 1, 1, now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.Fail(lineA, now); err != nil {
		t.Fatal(err)
	}
	// 同一故障重复上报，不能叠加相同停泵任务。
	if _, err := h.book.Fail(lineA, now); err != nil {
		t.Fatal(err)
	}
	pending := h.queue.Pending()
	if len(pending) != 1 {
		t.Fatalf("pending len = %d, want 1", len(pending))
	}
}

func TestCompensationStopsPumpWhenOwnershipMatches(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	h := newTestHarness(t)
	sequence, err := h.book.Start(lineA, pumpA, 1, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.ConfirmFlow(lineA, sequence.ID, 1, 120, now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.Fail(lineA, now); err != nil {
		t.Fatal(err)
	}
	dueAt := h.queue.Pending()[0].DueAt
	if _, err := execute(t, h.queue, h.book, dueAt); err != nil {
		t.Fatalf("execute: %v", err)
	}
	pump, _ := h.pumps.Owner(pumpA)
	if pump.Running {
		t.Fatal("pump was not stopped by compensation")
	}
	if len(h.queue.Pending()) != 0 {
		t.Fatalf("queue not drained: %+v", h.queue.Pending())
	}
}

func TestCompensationRequiresReviewWhenOwnershipMoves(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	h := newTestHarness(t)
	first, err := h.book.Start(lineA, pumpA, 1, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.ConfirmFlow(lineA, first.ID, 1, 120, now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.Fail(lineA, now); err != nil {
		t.Fatal(err)
	}

	// 故障后泵被另一个序列接管（例如重新排泥），补偿绝不能误停新任务。
	second, err := h.book.Start(lineA, pumpA, 2, 1, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.ConfirmFlow(lineA, second.ID, 1, 130, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	dueAt := h.queue.Pending()[0].DueAt
	ran, err := execute(t, h.queue, h.book, dueAt)
	if err != nil || ran != 0 {
		t.Fatalf("execute = %d, %v; want manual review without error", ran, err)
	}
	got := h.queue.Pending()
	if len(got) != 1 || got[0].Status != journal.CompensationManualReview {
		t.Fatalf("record = %+v, want manual_review", got)
	}
	if got[0].LastError == "" {
		t.Fatal("manual review reason was not recorded")
	}
	pump, _ := h.pumps.Owner(pumpA)
	if !pump.Running || pump.Flow != 130 {
		t.Fatalf("new owner's pump was disturbed: %+v", pump)
	}
}

func TestCompensationResumesAfterRestartAndStopsUnownedPump(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	h := newTestHarness(t)
	sequence, err := h.book.Start(lineA, pumpA, 1, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.ConfirmFlow(lineA, sequence.ID, 1, 120, now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.Fail(lineA, now); err != nil {
		t.Fatal(err)
	}
	item := h.queue.Pending()[0]

	// 进程重启：内存归属全部丢失，现场泵仍在转；补偿任务从磁盘恢复。
	restartedBook, restartedPumps, restartedQueue := h.reopen(t,
		sludge.Pump{ID: pumpA, LineID: lineA, Kind: model.DevicePump, Running: true, Flow: 120},
		sludge.Pump{ID: pumpB, LineID: lineA, Kind: model.DevicePump},
	)
	reloaded := restartedQueue.Pending()
	if len(reloaded) != 1 || reloaded[0].ID != item.ID {
		t.Fatalf("reloaded queue = %+v", reloaded)
	}
	if _, err := execute(t, restartedQueue, restartedBook, item.DueAt); err != nil {
		t.Fatalf("execute after restart: %v", err)
	}
	pump, _ := restartedPumps.Owner(pumpA)
	if pump.Running || pump.Flow != 0 {
		t.Fatalf("unowned pump was not stopped after restart: %+v", pump)
	}
	if len(restartedQueue.Pending()) != 0 {
		t.Fatalf("queue not drained after restart: %+v", restartedQueue.Pending())
	}
}

func TestCompensationAfterRestartReviewsWhenPumpReassigned(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	h := newTestHarness(t)
	sequence, err := h.book.Start(lineA, pumpA, 1, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.ConfirmFlow(lineA, sequence.ID, 1, 120, now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.book.Fail(lineA, now); err != nil {
		t.Fatal(err)
	}
	item := h.queue.Pending()[0]

	// 重启后、补偿执行前，新序列重新接管了泵。
	restartedBook, restartedPumps, restartedQueue := h.reopen(t,
		sludge.Pump{ID: pumpA, LineID: lineA, Kind: model.DevicePump, Running: true, Flow: 90},
		sludge.Pump{ID: pumpB, LineID: lineA, Kind: model.DevicePump},
	)
	newSequence, err := restartedBook.Start(lineA, pumpA, 9, 1, item.DueAt.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restartedBook.ConfirmFlow(lineA, newSequence.ID, 1, 140, item.DueAt.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	ran, err := execute(t, restartedQueue, restartedBook, item.DueAt)
	if err != nil || ran != 0 {
		t.Fatalf("execute = %d, %v; want manual review", ran, err)
	}
	got := restartedQueue.Pending()
	if len(got) != 1 || got[0].Status != journal.CompensationManualReview {
		t.Fatalf("record = %+v, want manual_review", got)
	}
	pump, _ := restartedPumps.Owner(pumpA)
	if !pump.Running || pump.OwnerSequence != newSequence.ID {
		t.Fatalf("new owner pump disturbed: %+v", pump)
	}
}
