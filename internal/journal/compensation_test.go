package journal

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func testCompensation(t *testing.T, due time.Time) Compensation {
	t.Helper()
	item, err := NewCompensation("line-1", uuid.New(), 3, "pump-line-1-A", "stop", due)
	if err != nil {
		t.Fatalf("new compensation: %v", err)
	}
	return item
}

func TestScheduleRejectsDuplicateFault(t *testing.T) {
	queue := NewCompensationQueue()
	item := testCompensation(t, time.Now())
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	// 用新的 ID 模拟同一故障再次产生停泵任务，必须被去重而不是叠加。
	item.ID = uuid.New()
	if _, err := queue.Schedule(item); !errors.Is(err, ErrCompensationDuplicate) {
		t.Fatalf("expected ErrCompensationDuplicate, got %v", err)
	}
	if len(queue.Pending()) != 1 {
		t.Fatalf("expected one pending task, got %d", len(queue.Pending()))
	}
}

func TestExecuteDueSuccessRemovesTask(t *testing.T) {
	queue := NewCompensationQueue()
	now := time.Now().UTC()
	item := testCompensation(t, now.Add(-time.Second))
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	executed := 0
	count, err := queue.ExecuteDue(context.Background(), now, func(_ context.Context, got Compensation) error {
		executed++
		if got.ID != item.ID {
			t.Errorf("executed unexpected task %s", got.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count != 1 || executed != 1 {
		t.Fatalf("expected one execution, count=%d executed=%d", count, executed)
	}
	if len(queue.Pending()) != 0 {
		t.Fatalf("successful task must be removed")
	}
}

func TestRetryableFailureBacksOffAndCountsAttempts(t *testing.T) {
	queue := NewCompensationQueue()
	now := time.Now().UTC()
	item := testCompensation(t, now)
	item.MaxAttempts = 2
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	if _, err := queue.ExecuteDue(context.Background(), now, func(context.Context, Compensation) error {
		return errors.New("device bus timeout")
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	pending := queue.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected task to stay pending after retryable failure")
	}
	if pending[0].Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", pending[0].Attempts)
	}
	if !pending[0].NextRetryAt.After(now) {
		t.Fatalf("expected next retry to be scheduled in the future, got %s", pending[0].NextRetryAt)
	}
	if pending[0].LastError != "device bus timeout" {
		t.Fatalf("expected failure reason recorded, got %q", pending[0].LastError)
	}

	// 退避窗口内不应执行。
	if _, err := queue.ExecuteDue(context.Background(), now.Add(time.Millisecond), func(context.Context, Compensation) error {
		t.Fatal("task must not run while backing off")
		return nil
	}); err != nil {
		t.Fatalf("execute during backoff: %v", err)
	}

	// 第二次失败耗尽重试次数，转人工复核。
	_, err := queue.ExecuteDue(context.Background(), now.Add(time.Hour), func(context.Context, Compensation) error {
		return errors.New("device bus timeout")
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	all := queue.List()
	if len(all) != 1 || all[0].Status != CompensationReview {
		t.Fatalf("expected manual_review after retry exhaustion, got %+v", all)
	}
	if all[0].NextRetryAt != (time.Time{}) {
		t.Fatalf("review task must have no next retry time")
	}
}

func TestManualReviewErrorStopsRetriesImmediately(t *testing.T) {
	queue := NewCompensationQueue()
	now := time.Now().UTC()
	item := testCompensation(t, now)
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	calls := 0
	if _, err := queue.ExecuteDue(context.Background(), now, func(context.Context, Compensation) error {
		calls++
		return NewManualReviewError("pump ownership changed before stop")
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if _, err := queue.ExecuteDue(context.Background(), now.Add(time.Hour), func(context.Context, Compensation) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if calls != 1 {
		t.Fatalf("reviewed task must never execute again, calls=%d", calls)
	}
	all := queue.List()
	if len(all) != 1 || all[0].Status != CompensationReview {
		t.Fatalf("expected manual_review status, got %+v", all)
	}
}

func TestPersistedQueueSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compensations")
	queue, err := NewPersistedCompensationQueue(dir)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	now := time.Now().UTC()
	item := testCompensation(t, now.Add(time.Minute))
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	// 模拟进程重启：新实例从同一文件恢复。
	reopened, err := NewPersistedCompensationQueue(dir)
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	pending := reopened.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected one recovered task, got %d", len(pending))
	}
	recovered := pending[0]
	if recovered.ID != item.ID || recovered.Attempts != 0 || recovered.Status != CompensationPending {
		t.Fatalf("recovered task mismatch: %+v", recovered)
	}
	if !recovered.NextRetryAt.Equal(item.NextRetryAt) {
		t.Fatalf("recovered next retry %s != %s", recovered.NextRetryAt, item.NextRetryAt)
	}

	// 重启后任务到期应继续执行并成功清理。
	count, err := reopened.ExecuteDue(context.Background(), now.Add(2*time.Minute), func(context.Context, Compensation) error {
		return nil
	})
	if err != nil {
		t.Fatalf("execute after restart: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected recovered task to execute, count=%d", count)
	}

	again, err := NewPersistedCompensationQueue(dir)
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	if len(again.List()) != 0 {
		t.Fatalf("completed task must not survive restart")
	}
}

func TestPersistedRetryStateSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compensations")
	queue, err := NewPersistedCompensationQueue(dir)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	queue.maxAttempts = 5
	now := time.Now().UTC()
	item := testCompensation(t, now)
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if _, err := queue.ExecuteDue(context.Background(), now, func(context.Context, Compensation) error {
		return errors.New("transient")
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	reopened, err := NewPersistedCompensationQueue(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	all := reopened.List()
	if len(all) != 1 || all[0].Attempts != 1 || all[0].LastError != "transient" {
		t.Fatalf("recovered retry state mismatch: %+v", all)
	}
	if all[0].NextRetryAt.IsZero() {
		t.Fatalf("recovered task must keep next retry time")
	}
}

func TestResolveReviewRemovesPersistedTask(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compensations")
	queue, err := NewPersistedCompensationQueue(dir)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	now := time.Now().UTC()
	item := testCompensation(t, now)
	if _, err := queue.Schedule(item); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := queue.MarkReview(item, "ownership moved"); err != nil {
		t.Fatalf("mark review: %v", err)
	}
	resolved, err := queue.ResolveReview(item.ID)
	if err != nil || !resolved {
		t.Fatalf("resolve: %v %v", resolved, err)
	}
	reopened, err := NewPersistedCompensationQueue(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(reopened.List()) != 0 {
		t.Fatalf("resolved review must be removed from disk")
	}
}

func TestFaultKeyIdentity(t *testing.T) {
	seq := uuid.New()
	first, err := NewCompensation("line-9", seq, 2, "pump-x", "stop", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCompensation("line-9", seq, 2, "pump-x", "stop", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if first.FaultKey() != second.FaultKey() {
		t.Fatalf("same fault must produce same key: %q vs %q", first.FaultKey(), second.FaultKey())
	}
	different, err := NewCompensation(model.LineID("line-9"), seq, 3, "pump-x", "stop", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if first.FaultKey() == different.FaultKey() {
		t.Fatalf("new generation must be a distinct fault")
	}
}
