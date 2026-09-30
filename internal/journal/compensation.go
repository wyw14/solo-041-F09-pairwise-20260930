package journal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type CompensationStatus string

const (
	// CompensationPending 等待到期执行或等待重试。
	CompensationPending CompensationStatus = "pending"
	// CompensationSucceeded 已成功完成，不再保留在活动队列中。
	CompensationSucceeded CompensationStatus = "succeeded"
	// CompensationManualReview 无法自动执行（如设备归属已变化），转人工复核，不再自动重试。
	CompensationManualReview CompensationStatus = "manual_review"
)

// DefaultMaxCompensationAttempts 单条补偿最大自动执行次数，超过后转人工复核。
const DefaultMaxCompensationAttempts = 5

type Compensation struct {
	ID          uuid.UUID          `json:"id"`
	LineID      model.LineID       `json:"line_id"`
	SequenceID  uuid.UUID          `json:"sequence_id"`
	Generation  uint64             `json:"generation"`
	DeviceID    string             `json:"device_id"`
	Action      string             `json:"action"`
	Status      CompensationStatus `json:"status"`
	Attempts    int                `json:"attempts"`
	DueAt       time.Time          `json:"due_at"`
	LastError   string             `json:"last_error,omitempty"`
	UpdatedAt   time.Time          `json:"updated_at"`
	SucceededAt *time.Time         `json:"succeeded_at,omitempty"`
}

func NewCompensation(lineID model.LineID, sequenceID uuid.UUID, generation uint64, deviceID, action string, dueAt time.Time) (Compensation, error) {
	if lineID == "" || sequenceID == uuid.Nil || generation == 0 || deviceID == "" || action == "" {
		return Compensation{}, errors.New("compensation identity is incomplete")
	}
	return Compensation{
		ID: uuid.New(), LineID: lineID, SequenceID: sequenceID, Generation: generation,
		DeviceID: deviceID, Action: action, Status: CompensationPending,
		DueAt: dueAt.UTC(), UpdatedAt: dueAt.UTC(),
	}, nil
}

// identity 标识“同一故障产生的同一停泵任务”，用于去重。
func (c Compensation) identity() string {
	return strings.Join([]string{
		string(c.LineID), c.SequenceID.String(), fmt.Sprintf("%d", c.Generation), c.DeviceID, c.Action,
	}, "|")
}

// ErrManualReview 表示补偿确认无法自动安全执行，必须转人工复核。
var ErrManualReview = errors.New("compensation requires manual review")

// ManualReviewError 携带需要人工介入的具体原因，errors.Is 可匹配 ErrManualReview。
type ManualReviewError struct {
	Reason string
}

func NewManualReviewError(reason string) error {
	return &ManualReviewError{Reason: strings.TrimSpace(reason)}
}

func (e *ManualReviewError) Error() string {
	if e.Reason == "" {
		return ErrManualReview.Error()
	}
	return ErrManualReview.Error() + ": " + e.Reason
}

func (e *ManualReviewError) Is(target error) bool { return target == ErrManualReview }

type CompensationExecutor func(context.Context, Compensation) error

type CompensationQueue struct {
	mu          sync.Mutex
	store       *CompensationStore
	active      []Compensation // pending 与 manual_review；succeeded 落盘即移除
	maxAttempts int
	baseBackoff time.Duration
}

type CompensationQueueOption func(*CompensationQueue)

// WithCompensationAttempts 覆盖最大自动执行次数。
func WithCompensationAttempts(maxAttempts int) CompensationQueueOption {
	return func(q *CompensationQueue) {
		if maxAttempts > 0 {
			q.maxAttempts = maxAttempts
		}
	}
}

// WithCompensationBackoff 覆盖首次重试退避基准（之后按 2 的幂递增，封顶 1 分钟）。
func WithCompensationBackoff(base time.Duration) CompensationQueueOption {
	return func(q *CompensationQueue) {
		if base > 0 {
			q.baseBackoff = base
		}
	}
}

// NewCompensationQueue 打开持久化队列并恢复未落盘完成的补偿任务，进程重启后可继续处理。
func NewCompensationQueue(store *CompensationStore, opts ...CompensationQueueOption) (*CompensationQueue, error) {
	if store == nil {
		return nil, errors.New("compensation store is required")
	}
	queue := &CompensationQueue{store: store, maxAttempts: DefaultMaxCompensationAttempts, baseBackoff: time.Second}
	for _, option := range opts {
		option(queue)
	}
	records, err := store.Load()
	if err != nil {
		return nil, err
	}
	queue.active = make([]Compensation, 0, len(records))
	for _, record := range records {
		if record.Status == "" {
			record.Status = CompensationPending
		}
		if record.Status == CompensationSucceeded {
			continue
		}
		if record.Status != CompensationPending && record.Status != CompensationManualReview {
			record.Status = CompensationPending
		}
		queue.active = append(queue.active, record)
	}
	queue.sortLocked()
	return queue, nil
}

// Schedule 入队一条补偿。同一故障（线/序列/代际/设备/动作相同）已有活动任务时不叠加。
// 返回 true 表示新入队，false 表示命中去重被忽略。
func (q *CompensationQueue) Schedule(item Compensation) (bool, error) {
	if item.ID == uuid.Nil {
		return false, errors.New("compensation identity is incomplete")
	}
	if item.Status == "" {
		item.Status = CompensationPending
	}
	if item.Status != CompensationPending {
		return false, errors.New("only pending compensation can be scheduled")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, existing := range q.active {
		if existing.identity() == item.identity() {
			return false, nil
		}
	}
	q.active = append(q.active, item)
	q.sortLocked()
	if err := q.persistLocked(); err != nil {
		return false, err
	}
	return true, nil
}

// ExecuteDue 执行所有到期的 pending 任务：
//   - 成功：标记 succeeded 并从活动队列移除；
//   - executor 返回 ErrManualReview，或自动执行次数达到上限：标记 manual_review 且不再重试；
//   - 其他错误：记录失败原因，按指数退避推后 DueAt 后继续重试；
//   - ctx 取消：本次尝试不计数，任务原样回到队列。
//
// 不因单条失败中断后续任务，返回遇到的第一个瞬时错误。
func (q *CompensationQueue) ExecuteDue(ctx context.Context, now time.Time, execute CompensationExecutor) (int, error) {
	if execute == nil {
		return 0, errors.New("compensation executor is required")
	}
	q.mu.Lock()
	due := make([]Compensation, 0, len(q.active))
	kept := make([]Compensation, 0, len(q.active))
	for _, item := range q.active {
		if item.Status == CompensationPending && !item.DueAt.After(now) {
			due = append(due, item)
		} else {
			kept = append(kept, item)
		}
	}
	q.active = kept
	q.mu.Unlock()

	executed := 0
	var firstErr error
	for index := 0; index < len(due); index++ {
		if err := ctx.Err(); err != nil {
			// 关停等取消场景：未执行的任务全部原样归还，本次不计数。
			if putErr := q.replaceAll(append(due[index:], q.snapshotActive()...)); putErr != nil {
				return executed, err
			}
			return executed, err
		}
		item := due[index]
		item.Attempts++
		item.UpdatedAt = now.UTC()
		err := execute(ctx, item)
		if ctxErr := ctx.Err(); ctxErr != nil {
			// executor 执行期间取消：本次尝试不计数，任务原样归还。
			item.Attempts--
			item.UpdatedAt = now.UTC()
			if putErr := q.replaceAll(append(due[index:], q.snapshotActive()...)); putErr != nil {
				return executed, ctxErr
			}
			return executed, ctxErr
		}
		switch {
		case err == nil:
			succeededAt := now.UTC()
			item.Status = CompensationSucceeded
			item.LastError = ""
			item.DueAt = succeededAt
			item.SucceededAt = &succeededAt
			if persistErr := q.drop(item.ID); persistErr != nil && firstErr == nil {
				firstErr = persistErr
			}
			executed++
		case errors.Is(err, ErrManualReview) || item.Attempts >= q.maxAttempts:
			item.Status = CompensationManualReview
			item.LastError = failureReason(err, item.Attempts >= q.maxAttempts && !errors.Is(err, ErrManualReview))
			if persistErr := q.upsert(item); persistErr != nil && firstErr == nil {
				firstErr = persistErr
			}
		default:
			item.LastError = err.Error()
			item.DueAt = q.backoff(now, item.Attempts)
			if persistErr := q.upsert(item); persistErr != nil && firstErr == nil {
				firstErr = persistErr
			}
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return executed, firstErr
}

func failureReason(err error, attemptsExhausted bool) string {
	if attemptsExhausted && !errors.Is(err, ErrManualReview) {
		return fmt.Sprintf("max attempts exhausted: %s", err.Error())
	}
	return err.Error()
}

func (q *CompensationQueue) backoff(now time.Time, attempts int) time.Time {
	delay := q.baseBackoff
	for step := 1; step < attempts && delay < time.Minute; step++ {
		delay *= 2
	}
	if delay > time.Minute {
		delay = time.Minute
	}
	return now.Add(delay).UTC()
}

// Pending 返回全部活动补偿（含 pending 与 manual_review），状态页用它展示执行次数、下次重试时间与失败原因。
func (q *CompensationQueue) Pending() []Compensation {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]Compensation, 0, len(q.active))
	return append(result, q.active...)
}

func (q *CompensationQueue) upsert(item Compensation) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for index, existing := range q.active {
		if existing.ID == item.ID {
			q.active[index] = item
			q.sortLocked()
			return q.persistLocked()
		}
	}
	q.active = append(q.active, item)
	q.sortLocked()
	return q.persistLocked()
}

func (q *CompensationQueue) drop(id uuid.UUID) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	kept := q.active[:0]
	for _, item := range q.active {
		if item.ID != id {
			kept = append(kept, item)
		}
	}
	q.active = kept
	return q.persistLocked()
}

func (q *CompensationQueue) replaceAll(items []Compensation) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.active = append([]Compensation(nil), items...)
	q.sortLocked()
	return q.persistLocked()
}

func (q *CompensationQueue) snapshotActive() []Compensation {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]Compensation(nil), q.active...)
}

func (q *CompensationQueue) persistLocked() error {
	return q.store.Save(q.active)
}

// sortLocked 先按状态（pending 在前），再按到期时间，保证最早到期优先执行。
func (q *CompensationQueue) sortLocked() {
	sort.SliceStable(q.active, func(i, j int) bool {
		if q.active[i].Status != q.active[j].Status {
			return q.active[i].Status == CompensationPending
		}
		if q.active[i].DueAt.Equal(q.active[j].DueAt) {
			return q.active[i].ID.String() < q.active[j].ID.String()
		}
		return q.active[i].DueAt.Before(q.active[j].DueAt)
	})
}
