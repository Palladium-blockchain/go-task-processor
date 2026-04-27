package postgres

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Palladium-blockchain/go-task-processor/pkg/processor"
	"github.com/google/uuid"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestCreateAppliesDefaults(t *testing.T) {
	storage, mock := newMockStorage(t)

	returnedID := uuid.New()
	createdAt := time.Date(2026, 4, 27, 19, 0, 0, 0, time.UTC)

	mock.ExpectQuery(sqlPattern(`
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
	`)).
		WithArgs(
			sqlmock.AnyArg(),
			processor.TaskStatusPending,
			"email.send",
			[]byte{},
			0,
			defaultMaxAttempts,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "status", "type", "payload", "attempts", "max_attempts", "created_at",
		}).AddRow(
			returnedID,
			processor.TaskStatusPending,
			"email.send",
			[]byte{},
			0,
			defaultMaxAttempts,
			createdAt,
		))

	task, err := storage.Create(context.Background(), processor.Task{
		Type: "email.send",
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	if task.ID != processor.TaskID(returnedID) {
		t.Fatalf("unexpected task ID: got %v want %v", uuid.UUID(task.ID), returnedID)
	}
	if task.Status != processor.TaskStatusPending {
		t.Fatalf("unexpected status: got %d want %d", task.Status, processor.TaskStatusPending)
	}
	if task.MaxAttempts != defaultMaxAttempts {
		t.Fatalf("unexpected max attempts: got %d want %d", task.MaxAttempts, defaultMaxAttempts)
	}
	if len(task.Payload) != 0 {
		t.Fatalf("expected empty payload by default, got %v", task.Payload)
	}

	assertMockExpectations(t, mock)
}

func TestTryLockReturnsFalseWhenTaskWasNotClaimed(t *testing.T) {
	storage, mock := newMockStorage(t)
	taskID := uuid.New()

	mock.ExpectQuery(sqlPattern(`
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
	`)).
		WithArgs(
			processor.TaskStatusRunning,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			taskID,
			processor.TaskStatusPending,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "status", "type", "payload", "attempts", "max_attempts", "created_at",
		}))

	_, ok, err := storage.TryLock(context.Background(), processor.TaskID(taskID), 30*time.Second)
	if err != nil {
		t.Fatalf("TryLock returned error: %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to report task as not claimed")
	}

	assertMockExpectations(t, mock)
}

func TestFetchAndLockReturnsClaimedTasks(t *testing.T) {
	storage, mock := newMockStorage(t)
	firstID := uuid.New()
	secondID := uuid.New()
	firstCreatedAt := time.Date(2026, 4, 27, 19, 0, 0, 0, time.UTC)
	secondCreatedAt := firstCreatedAt.Add(time.Minute)

	mock.ExpectQuery(sqlPattern(`
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
	`)).
		WithArgs(
			processor.TaskStatusPending,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			2,
			processor.TaskStatusRunning,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "status", "type", "payload", "attempts", "max_attempts", "created_at",
		}).
			AddRow(firstID, processor.TaskStatusRunning, "first", []byte("a"), 1, 3, firstCreatedAt).
			AddRow(secondID, processor.TaskStatusRunning, "second", []byte("b"), 2, 5, secondCreatedAt))

	tasks, err := storage.FetchAndLock(context.Background(), 2, 30*time.Second)
	if err != nil {
		t.Fatalf("FetchAndLock returned error: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("unexpected number of tasks: got %d want 2", len(tasks))
	}
	if tasks[0].ID != processor.TaskID(firstID) || tasks[1].ID != processor.TaskID(secondID) {
		t.Fatalf("unexpected task IDs: %+v", tasks)
	}
	if tasks[1].Attempts != 2 || tasks[1].MaxAttempts != 5 {
		t.Fatalf("unexpected task counters: %+v", tasks[1])
	}

	assertMockExpectations(t, mock)
}

func TestCompleteClearsLease(t *testing.T) {
	storage, mock := newMockStorage(t)
	taskID := uuid.New()

	mock.ExpectExec(sqlPattern(`
		UPDATE tasks
		SET
			status = ?,
			locked_until = NULL,
			last_error = ?,
			updated_at = ?
		WHERE id = ?
		  AND status = ?
	`)).
		WithArgs(
			processor.TaskStatusDone,
			nil,
			sqlmock.AnyArg(),
			taskID,
			processor.TaskStatusRunning,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := storage.Complete(context.Background(), processor.TaskID(taskID)); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	assertMockExpectations(t, mock)
}

func TestRetryReschedulesTask(t *testing.T) {
	storage, mock := newMockStorage(t)
	taskID := uuid.New()

	mock.ExpectExec(sqlPattern(`
		UPDATE tasks
		SET
			status = ?,
			run_at = ?,
			locked_until = NULL,
			last_error = ?,
			updated_at = ?
		WHERE id = ?
		  AND status = ?
	`)).
		WithArgs(
			processor.TaskStatusPending,
			sqlmock.AnyArg(),
			"temporary error",
			sqlmock.AnyArg(),
			taskID,
			processor.TaskStatusRunning,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := storage.Retry(context.Background(), processor.TaskID(taskID), "temporary error"); err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}

	assertMockExpectations(t, mock)
}

func TestFailPersistsLastError(t *testing.T) {
	storage, mock := newMockStorage(t)
	taskID := uuid.New()

	mock.ExpectExec(sqlPattern(`
		UPDATE tasks
		SET
			status = ?,
			locked_until = NULL,
			last_error = ?,
			updated_at = ?
		WHERE id = ?
		  AND status = ?
	`)).
		WithArgs(
			processor.TaskStatusFailed,
			"permanent error",
			sqlmock.AnyArg(),
			taskID,
			processor.TaskStatusRunning,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := storage.Fail(context.Background(), processor.TaskID(taskID), "permanent error"); err != nil {
		t.Fatalf("Fail returned error: %v", err)
	}

	assertMockExpectations(t, mock)
}

func newMockStorage(t *testing.T) (*Storage, sqlmock.Sqlmock) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}

	gormDB, err := gorm.Open(gormpostgres.New(gormpostgres.Config{
		Conn:                 sqlDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("gorm.Open failed: %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	return New(gormDB), mock
}

func assertMockExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func sqlPattern(query string) string {
	normalized := strings.Join(strings.Fields(query), " ")
	quoted := regexp.QuoteMeta(normalized)
	return strings.ReplaceAll(quoted, `\?`, `\$[0-9]+`)
}
