package journal_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/journal"

	"github.com/google/uuid"
)

func mustUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if id == uuid.Nil {
		t.Fatal("uuid generation failed")
	}
	return id
}

func newTestCompensation(t *testing.T, device string) journal.Compensation {
	t.Helper()
	item, err := journal.NewCompensation("line-1", mustUUID(t), 7, device, "stop", time.Unix(100, 0).UTC())
	if err != nil {
		t.Fatalf("new compensation: %v", err)
	}
	return item
}

func TestCompensationScheduleDedupesAndPersists(t *testing.T) {
	store, err := journal.NewCompensationStore(filepath.Join(t.TempDir(), "compensations.json"))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	item := newTestCompensation(t, "pump-line-1-A")
	scheduled, err := queue.Schedule(item)
	if err != nil || !scheduled {
		t.Fatalf("first schedule = %v, %v; want true, nil", scheduled, err)
	}
	// 同一故障重复入队必须被去重，不能叠加相同停泵任务。
	again := item
	again.ID = mustUUID(t) // 即使新生成 ID，身份键仍相同
	again.DueAt = item.DueAt.Add(time.Hour)
	scheduled, err = queue.Schedule(again)
	if err != nil || scheduled {
		t.Fatalf("duplicate schedule = %v, %v; want false, nil", scheduled, err)
	}
	if pending := queue.Pending(); len(pending) != 1 {
		t.Fatalf("pending len = %d, want 1", len(pending))
	}

	// 重新打开队列（模拟进程重启），任务应原样恢复。
	reopened, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	pending := reopened.Pending()
	if len(pending) != 1 || pending[0].ID != item.ID || pending[0].Status != journal.CompensationPending {
		t.Fatalf("reloaded pending = %+v, want original item", pending)
	}
}

func TestCompensationRetryBackoffThenSuccessAcrossRestart(t *testing.T) {
	store, err := journal.NewCompensationStore(filepath.Join(t.TempDir(), "compensations.json"))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := journal.NewCompensationQueue(store, journal.WithCompensationBackoff(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1000, 0).UTC()
	item := newTestCompensation(t, "pump-line-1-A")
	item.DueAt = base
	if _, err := queue.Schedule(item); err != nil {
		t.Fatal(err)
	}

	attempt := 0
	flaky := func(_ context.Context, _ journal.Compensation) error {
		attempt++
		if attempt < 3 {
			return errors.New("pump not reachable")
		}
		return nil
	}

	// 第一次执行失败：次数 +1，下次重试被推后，任务保留为 pending。
	if _, err := queue.ExecuteDue(context.Background(), base, flaky); err == nil {
		t.Fatal("expected executor error to surface")
	}
	if got := queue.Pending(); len(got) != 1 || got[0].Attempts != 1 || got[0].LastError != "pump not reachable" || !got[0].DueAt.After(base) {
		t.Fatalf("after first failure: %+v", got)
	}

	// 重启：状态、执行次数、下次重试时间从磁盘恢复。
	queue, err = journal.NewCompensationQueue(store, journal.WithCompensationBackoff(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	pending := queue.Pending()
	if len(pending) != 1 || pending[0].Attempts != 1 || pending[0].LastError != "pump not reachable" {
		t.Fatalf("reloaded record = %+v", pending[0])
	}

	// 未到期不执行。
	if _, err := queue.ExecuteDue(context.Background(), base.Add(time.Nanosecond), flaky); err != nil {
		t.Fatalf("not due execution: %v", err)
	}
	if queue.Pending()[0].Attempts != 1 {
		t.Fatal("task executed before due time")
	}

	// 到期后继续重试直到成功，成功任务从活动队列移除并落盘。
	executed, err := queue.ExecuteDue(context.Background(), pending[0].DueAt, flaky) // 第 2 次仍失败
	if err == nil {
		t.Fatal("expected second failure")
	}
	dueAt := queue.Pending()[0].DueAt
	executed, err = queue.ExecuteDue(context.Background(), dueAt, flaky) // 第 3 次成功
	if err != nil || executed != 1 {
		t.Fatalf("final execution = %d, %v; want 1, nil", executed, err)
	}
	if pending := queue.Pending(); len(pending) != 0 {
		t.Fatalf("succeeded task left in queue: %+v", pending)
	}

	reopened, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Pending()) != 0 {
		t.Fatalf("succeeded task survived restart: %+v", reopened.Pending())
	}
}

func TestCompensationManualReviewIsTerminal(t *testing.T) {
	store, err := journal.NewCompensationStore(filepath.Join(t.TempDir(), "compensations.json"))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(500, 0).UTC()
	item := newTestCompensation(t, "pump-line-1-A")
	item.DueAt = now
	if _, err := queue.Schedule(item); err != nil {
		t.Fatal(err)
	}

	called := 0
	executor := func(_ context.Context, _ journal.Compensation) error {
		called++
		return journal.NewManualReviewError("pump ownership moved")
	}
	if _, err := queue.ExecuteDue(context.Background(), now, executor); err != nil {
		t.Fatalf("manual review should not surface as error: %v", err)
	}
	if called != 1 {
		t.Fatalf("executor called %d times, want 1", called)
	}
	pending := queue.Pending()
	if len(pending) != 1 || pending[0].Status != journal.CompensationManualReview {
		t.Fatalf("record = %+v, want manual_review", pending)
	}
	if pending[0].LastError == "" {
		t.Fatal("manual review reason was not persisted")
	}

	// 之后的维护周期不应再自动尝试人工复核任务。
	if _, err := queue.ExecuteDue(context.Background(), now.Add(time.Hour), executor); err != nil {
		t.Fatalf("review run: %v", err)
	}
	if called != 1 {
		t.Fatalf("manual review task retried %d times", called)
	}

	// 重启后仍然停留在人工复核。
	reopened, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	got := reopened.Pending()
	if len(got) != 1 || got[0].Status != journal.CompensationManualReview {
		t.Fatalf("reloaded record = %+v", got)
	}
}

func TestCompensationAttemptsExhaustionMovesToReview(t *testing.T) {
	store, err := journal.NewCompensationStore(filepath.Join(t.TempDir(), "sub", "compensations.json"))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := journal.NewCompensationQueue(store,
		journal.WithCompensationAttempts(2), journal.WithCompensationBackoff(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).UTC()
	item := newTestCompensation(t, "pump-line-1-B")
	item.DueAt = now
	if _, err := queue.Schedule(item); err != nil {
		t.Fatal(err)
	}
	failing := func(_ context.Context, _ journal.Compensation) error { return errors.New("boom") }
	if _, err := queue.ExecuteDue(context.Background(), now, failing); err == nil {
		t.Fatal("want failure surfaced on attempt 1 while still retrying")
	}
	next := queue.Pending()[0].DueAt
	if _, err := queue.ExecuteDue(context.Background(), next, failing); err != nil {
		t.Fatalf("attempt exhaustion should become manual review, not error: %v", err)
	}
	got := queue.Pending()
	if len(got) != 1 || got[0].Status != journal.CompensationManualReview || got[0].Attempts != 2 {
		t.Fatalf("record after exhaustion = %+v", got[0])
	}
	if !strings.Contains(got[0].LastError, "boom") || !strings.Contains(got[0].LastError, "max attempts") {
		t.Fatalf("exhaustion reason = %q, want last failure with max attempts note", got[0].LastError)
	}
}

func TestCompensationCancelledContextDoesNotCount(t *testing.T) {
	store, err := journal.NewCompensationStore(filepath.Join(t.TempDir(), "compensations.json"))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := journal.NewCompensationQueue(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	item := newTestCompensation(t, "pump-line-1-A")
	item.DueAt = now
	if _, err := queue.Schedule(item); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	executor := func(context.Context, journal.Compensation) error { called = true; return nil }
	if _, err := queue.ExecuteDue(ctx, now, executor); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	got := queue.Pending()
	if called || len(got) != 1 || got[0].Attempts != 0 || got[0].Status != journal.CompensationPending {
		t.Fatalf("record after cancel = %+v called=%v", got, called)
	}
}
