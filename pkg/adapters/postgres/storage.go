package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Palladium-blockchain/go-task-processor/pkg/processor"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Storage struct {
	db *gorm.DB
}

const defaultMaxAttempts = 3

func New(db *gorm.DB) *Storage {
	return &Storage{
		db: db,
	}
}

func (s *Storage) Create(ctx context.Context, task processor.Task) (processor.Task, error) {
	if s.db == nil {
		return processor.Task{}, errors.New("postgres storage: nil db")
	}

	now := time.Now().UTC()
	row := newDBTaskForCreate(task, now)

	result := s.db.WithContext(ctx).Raw(`
		INSERT INTO tasks (
			id,
			status,
			type,
			payload,
			attempts,
			max_attempts,
			run_at,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id, status, type, payload, attempts, max_attempts, created_at
	`,
		row.ID,
		row.Status,
		row.Type,
		row.Payload,
		row.Attempts,
		row.MaxAttempts,
		now,
		row.CreatedAt,
		now,
	).Scan(&row)
	if result.Error != nil {
		return processor.Task{}, result.Error
	}

	return row.toTask(), nil
}

func (s *Storage) TryLock(ctx context.Context, taskID processor.TaskID, lockTTL time.Duration) (processor.Task, bool, error) {
	if s.db == nil {
		return processor.Task{}, false, errors.New("postgres storage: nil db")
	}

	now := time.Now().UTC()
	lockUntil := now.Add(lockTTL)
	var row dbTask

	result := s.db.WithContext(ctx).Raw(`
		UPDATE tasks
		SET
			status = ?,
			attempts = attempts + 1,
			locked_until = ?,
			updated_at = ?
		WHERE id = ?
		  AND status = ?
		  AND run_at <= ?
		  AND (locked_until IS NULL OR locked_until < ?)
		RETURNING id, status, type, payload, attempts, max_attempts, created_at
	`,
		processor.TaskStatusRunning,
		lockUntil,
		now,
		taskIDToUUID(taskID),
		processor.TaskStatusPending,
		now,
		now,
	).Scan(&row)
	if result.Error != nil {
		return processor.Task{}, false, result.Error
	}
	if result.RowsAffected == 0 {
		return processor.Task{}, false, nil
	}

	return row.toTask(), true, nil
}

func (s *Storage) FetchAndLock(ctx context.Context, limit int, lockTTL time.Duration) ([]processor.Task, error) {
	if s.db == nil {
		return nil, errors.New("postgres storage: nil db")
	}
	if limit <= 0 {
		return []processor.Task{}, nil
	}

	now := time.Now().UTC()
	lockUntil := now.Add(lockTTL)
	var rows []dbTask

	result := s.db.WithContext(ctx).Raw(`
		WITH candidates AS (
			SELECT id
			FROM tasks
			WHERE status = ?
			  AND run_at <= ?
			  AND (locked_until IS NULL OR locked_until < ?)
			ORDER BY run_at, created_at
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		UPDATE tasks AS t
		SET
			status = ?,
			attempts = t.attempts + 1,
			locked_until = ?,
			updated_at = ?
		FROM candidates
		WHERE t.id = candidates.id
		RETURNING t.id, t.status, t.type, t.payload, t.attempts, t.max_attempts, t.created_at
	`,
		processor.TaskStatusPending,
		now,
		now,
		limit,
		processor.TaskStatusRunning,
		lockUntil,
		now,
	).Scan(&rows)
	if result.Error != nil {
		return nil, result.Error
	}

	tasks := make([]processor.Task, 0, len(rows))
	for _, row := range rows {
		tasks = append(tasks, row.toTask())
	}

	return tasks, nil
}

func (s *Storage) Complete(ctx context.Context, taskID processor.TaskID) error {
	return s.updateTerminalState(ctx, taskID, processor.TaskStatusDone, nil, true)
}

func (s *Storage) Retry(ctx context.Context, taskID processor.TaskID, reason string) error {
	if s.db == nil {
		return errors.New("postgres storage: nil db")
	}

	now := time.Now().UTC()

	return s.db.WithContext(ctx).Exec(`
		UPDATE tasks
		SET
			status = ?,
			run_at = ?,
			locked_until = NULL,
			last_error = ?,
			updated_at = ?
		WHERE id = ?
		  AND status = ?
	`,
		processor.TaskStatusPending,
		now,
		reason,
		now,
		taskIDToUUID(taskID),
		processor.TaskStatusRunning,
	).Error
}

func (s *Storage) Fail(ctx context.Context, taskID processor.TaskID, reason string) error {
	return s.updateTerminalState(ctx, taskID, processor.TaskStatusFailed, &reason, false)
}

type dbTask struct {
	ID          uuid.UUID `gorm:"column:id"`
	Status      int       `gorm:"column:status"`
	Type        string    `gorm:"column:type"`
	Payload     []byte    `gorm:"column:payload"`
	Attempts    int       `gorm:"column:attempts"`
	MaxAttempts int       `gorm:"column:max_attempts"`
	CreatedAt   time.Time `gorm:"column:created_at"`
}

func newDBTaskForCreate(task processor.Task, now time.Time) dbTask {
	id := task.ID
	if uuid.UUID(id) == uuid.Nil {
		id = processor.TaskID(uuid.New())
	}

	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	createdAt := task.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}

	payload := task.Payload
	if payload == nil {
		payload = []byte{}
	}

	return dbTask{
		ID:          taskIDToUUID(id),
		Status:      processor.TaskStatusPending,
		Type:        task.Type,
		Payload:     payload,
		Attempts:    0,
		MaxAttempts: maxAttempts,
		CreatedAt:   createdAt,
	}
}

func (t dbTask) toTask() processor.Task {
	return processor.Task{
		ID:          uuidToTaskID(t.ID),
		Status:      t.Status,
		Type:        t.Type,
		Payload:     t.Payload,
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		CreatedAt:   t.CreatedAt,
	}
}

func (s *Storage) updateTerminalState(
	ctx context.Context,
	taskID processor.TaskID,
	status int,
	lastError *string,
	clearLastError bool,
) error {
	if s.db == nil {
		return errors.New("postgres storage: nil db")
	}

	now := time.Now().UTC()
	var errValue any
	if clearLastError || lastError == nil {
		errValue = nil
	} else {
		errValue = *lastError
	}

	return s.db.WithContext(ctx).Exec(`
		UPDATE tasks
		SET
			status = ?,
			locked_until = NULL,
			last_error = ?,
			updated_at = ?
		WHERE id = ?
		  AND status = ?
	`,
		status,
		errValue,
		now,
		taskIDToUUID(taskID),
		processor.TaskStatusRunning,
	).Error
}

func taskIDToUUID(id processor.TaskID) uuid.UUID {
	return uuid.UUID(id)
}

func uuidToTaskID(id uuid.UUID) processor.TaskID {
	return processor.TaskID(id)
}
