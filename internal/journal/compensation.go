package journal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type CompensationStatus string

const (
	// CompensationPending 等待首次执行或退避后重试。
	CompensationPending CompensationStatus = "pending"
	// CompensationReview 不可自动恢复（设备归属已变化/设备缺失）或重试耗尽，等待人工复核。
	CompensationReview CompensationStatus = "manual_review"
)

// ErrManualReviewRequired 表示补偿任务命中不可恢复场景，必须人工介入。
// 例如泵已被新序列接管，继续自动停泵会误停不属于本故障的任务。
var ErrManualReviewRequired = errors.New("compensation requires manual review")

// ErrCompensationDuplicate 表示同一故障已有相同停泵任务，不叠加。
var ErrCompensationDuplicate = errors.New("compensation already scheduled for fault")

const (
	defaultMaxAttempts    = 5
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = 30 * time.Minute
	persistedName         = "compensations.json"
)

type Compensation struct {
	ID          uuid.UUID          `json:"id"`
	LineID      model.LineID       `json:"line_id"`
	SequenceID  uuid.UUID          `json:"sequence_id"`
	Generation  uint64             `json:"generation"`
	DeviceID    string             `json:"device_id"`
	Action      string             `json:"action"`
	Status      CompensationStatus `json:"status"`
	Attempts    uint               `json:"attempts"`
	MaxAttempts uint               `json:"max_attempts"`
	// DueAt 首次应执行时间；NextRetryAt 为退避后的下次重试时间。
	DueAt       time.Time `json:"due_at"`
	NextRetryAt time.Time `json:"next_retry_at,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// FaultKey 同一故障（处理线+序列+代际+设备+动作）的身份键，用于防止叠加相同停泵任务。
func (c Compensation) FaultKey() string {
	return strings.Join([]string{
		string(c.LineID), c.SequenceID.String(), fmt.Sprint(c.Generation), c.DeviceID, c.Action,
	}, "|")
}

func (c Compensation) retryTime() time.Time {
	if !c.NextRetryAt.IsZero() {
		return c.NextRetryAt
	}
	return c.DueAt
}

func NewCompensation(lineID model.LineID, sequenceID uuid.UUID, generation uint64, deviceID, action string, dueAt time.Time) (Compensation, error) {
	if lineID == "" || sequenceID == uuid.Nil || generation == 0 || deviceID == "" || action == "" {
		return Compensation{}, errors.New("compensation identity is incomplete")
	}
	dueAt = dueAt.UTC()
	return Compensation{
		ID: uuid.New(), LineID: lineID, SequenceID: sequenceID, Generation: generation,
		DeviceID: deviceID, Action: action, Status: CompensationPending,
		MaxAttempts: defaultMaxAttempts, DueAt: dueAt, NextRetryAt: dueAt,
		CreatedAt: dueAt, UpdatedAt: dueAt,
	}, nil
}

type CompensationExecutor func(context.Context, Compensation) error

// ManualReviewError 包装不可自动重试的执行错误（例如设备归属已变化）。
type ManualReviewError struct {
	Reason string
}

func NewManualReviewError(reason string) error {
	return &ManualReviewError{Reason: reason}
}

func (e *ManualReviewError) Error() string {
	if e.Reason == "" {
		return ErrManualReviewRequired.Error()
	}
	return ErrManualReviewRequired.Error() + ": " + e.Reason
}

func (e *ManualReviewError) Unwrap() error { return ErrManualReviewRequired }

type CompensationQueue struct {
	path           string
	maxAttempts    uint
	initialBackoff time.Duration
	maxBackoff     time.Duration
	mu             sync.Mutex
	items          map[string]Compensation
}

// NewCompensationQueue 创建纯内存队列（主要用于测试）。
// 生产环境应使用 NewPersistedCompensationQueue，进程重启后才能继续处理。
func NewCompensationQueue() *CompensationQueue {
	return &CompensationQueue{
		items:          make(map[string]Compensation, 8),
		maxAttempts:    defaultMaxAttempts,
		initialBackoff: defaultInitialBackoff,
		maxBackoff:     defaultMaxBackoff,
	}
}

// NewPersistedCompensationQueue 打开（或创建）dataDir 下的补偿任务文件并恢复未完成任务。
func NewPersistedCompensationQueue(dataDir string) (*CompensationQueue, error) {
	if dataDir == "" {
		return nil, errors.New("compensation data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create compensation directory: %w", err)
	}
	queue := NewCompensationQueue()
	queue.path = filepath.Join(dataDir, persistedName)
	if err := queue.load(); err != nil {
		return nil, err
	}
	return queue, nil
}

// Path 返回持久化文件位置（供启动顺序中恢复设备归属等场景使用）。
func (q *CompensationQueue) Path() string { return q.path }

func (q *CompensationQueue) backoff(attempt uint, now time.Time) time.Time {
	delay := q.initialBackoff
	for i := uint(0); i < attempt && delay < q.maxBackoff; i++ {
		delay *= 2
	}
	if delay > q.maxBackoff {
		delay = q.maxBackoff
	}
	return now.Add(delay)
}

// Schedule 入队一个补偿任务并立即落盘。同一故障已有任务（等待中或已转人工复核）
// 时返回 ErrCompensationDuplicate，不叠加相同停泵任务。
func (q *CompensationQueue) Schedule(item Compensation) (Compensation, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := item.FaultKey()
	if existing, ok := q.items[key]; ok {
		return existing, ErrCompensationDuplicate
	}
	item.DueAt = item.DueAt.UTC()
	if item.NextRetryAt.IsZero() {
		item.NextRetryAt = item.DueAt
	}
	item.NextRetryAt = item.NextRetryAt.UTC()
	if item.Status == "" {
		item.Status = CompensationPending
	}
	if item.MaxAttempts == 0 {
		item.MaxAttempts = q.maxAttempts
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = item.DueAt
	}
	item.UpdatedAt = item.DueAt
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	q.items[key] = item
	if err := q.persistLocked(); err != nil {
		delete(q.items, key)
		return Compensation{}, err
	}
	return item, nil
}

// ExecuteDue 执行所有 next_retry_at <= now 的 pending 任务：
//   - 成功：从队列移除；
//   - 命中 ErrManualReviewRequired：立即转人工复核并保留失败原因；
//   - 其他错误：按指数退避安排下次重试，次数耗尽后转人工复核。
//
// 每条任务的状态变化都立即落盘，进程崩溃重启后可继续处理。
func (q *CompensationQueue) ExecuteDue(ctx context.Context, now time.Time, execute CompensationExecutor) (int, error) {
	if execute == nil {
		return 0, errors.New("compensation executor is required")
	}
	now = now.UTC()
	q.mu.Lock()
	keys := q.dueKeysLocked(now)
	q.mu.Unlock()

	executed := 0
	var firstErr error
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return executed, err
		}
		q.mu.Lock()
		item, ok := q.items[key]
		if !ok || item.Status != CompensationPending {
			q.mu.Unlock()
			continue
		}
		item.Attempts++
		item.UpdatedAt = now
		q.items[key] = item
		if err := q.persistLocked(); err != nil {
			firstErr = err
			q.mu.Unlock()
			// 落盘失败：停止本轮执行，避免执行结果只存在于内存。
			break
		}
		q.mu.Unlock()

		err := execute(ctx, item)

		q.mu.Lock()
		switch {
		case err == nil:
			delete(q.items, key)
		case errors.Is(err, ErrManualReviewRequired):
			item.Status = CompensationReview
			item.LastError = err.Error()
			item.NextRetryAt = time.Time{}
			item.UpdatedAt = now
			q.items[key] = item
		default:
			item.LastError = err.Error()
			if item.Attempts >= item.MaxAttempts {
				item.Status = CompensationReview
				item.LastError = fmt.Sprintf("retry limit exhausted (%d attempts): %s", item.Attempts, err.Error())
				item.NextRetryAt = time.Time{}
			} else {
				item.NextRetryAt = q.backoff(item.Attempts, now)
			}
			item.UpdatedAt = now
			q.items[key] = item
		}
		if pErr := q.persistLocked(); pErr != nil && firstErr == nil {
			firstErr = pErr
		}
		q.mu.Unlock()
		if err == nil {
			executed++
		}
	}
	return executed, firstErr
}

func (q *CompensationQueue) dueKeysLocked(now time.Time) []string {
	due := make([]Compensation, 0, len(q.items))
	for _, item := range q.items {
		if item.Status == CompensationPending && !item.retryTime().After(now) {
			due = append(due, item)
		}
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i].retryTime().Before(due[j].retryTime()) })
	keys := make([]string, 0, len(due))
	for _, item := range due {
		keys = append(keys, item.FaultKey())
	}
	return keys
}

// MarkReview 将一条待执行任务直接标记为人工复核（用于进程重启后判定归属已变化）。
func (q *CompensationQueue) MarkReview(item Compensation, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := item.FaultKey()
	current, ok := q.items[key]
	if !ok {
		return nil
	}
	current.Status = CompensationReview
	if reason != "" {
		current.LastError = reason
	}
	current.NextRetryAt = time.Time{}
	q.items[key] = current
	return q.persistLocked()
}

// ResolveReview 人工确认处置后移除一条复核任务。任务不存在或无需复核时返回 false。
func (q *CompensationQueue) ResolveReview(id uuid.UUID) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for key, item := range q.items {
		if item.ID != id || item.Status != CompensationReview {
			continue
		}
		delete(q.items, key)
		return true, q.persistLocked()
	}
	return false, nil
}

// Pending 返回所有待执行（含退避等待重试）的补偿任务，按下次重试时间升序。
func (q *CompensationQueue) Pending() []Compensation {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]Compensation, 0, len(q.items))
	for _, item := range q.items {
		if item.Status == CompensationPending {
			result = append(result, item)
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].retryTime().Before(result[j].retryTime()) })
	return result
}

// List 返回全部补偿任务（含人工复核），按创建时间升序，供状态页展示。
func (q *CompensationQueue) List() []Compensation {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]Compensation, 0, len(q.items))
	for _, item := range q.items {
		result = append(result, item)
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}
