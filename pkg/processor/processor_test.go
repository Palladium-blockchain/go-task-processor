package processor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRegisterHandlerRejectsDuplicates(t *testing.T) {
	p := New(&fakeStorage{})

	handler := func(context.Context, Task) error { return nil }
	if err := p.RegisterHandler("email.send", handler); err != nil {
		t.Fatalf("first RegisterHandler returned error: %v", err)
	}

	err := p.RegisterHandler("email.send", handler)
	if err == nil {
		t.Fatal("expected duplicate RegisterHandler to fail")
	}
}

func TestNewUsesDefaultOptions(t *testing.T) {
	p := New(&fakeStorage{})

	if p.cfg.LockTTL != 10*time.Second {
		t.Fatalf("unexpected default lock TTL: got %s", p.cfg.LockTTL)
	}
	if p.cfg.PollInterval != time.Second {
		t.Fatalf("unexpected default poll interval: got %s", p.cfg.PollInterval)
	}
	if p.cfg.PollBatchSize != 10 {
		t.Fatalf("unexpected default poll batch size: got %d", p.cfg.PollBatchSize)
	}
	if p.cfg.Concurrency != 10 {
		t.Fatalf("unexpected default concurrency: got %d", p.cfg.Concurrency)
	}
	if cap(p.queue) != 20 {
		t.Fatalf("unexpected default queue size: got %d", cap(p.queue))
	}
}

func TestNewProcessorAppliesFunctionalOptions(t *testing.T) {
	p := New(
		&fakeStorage{},
		WithLockTTL(5*time.Second),
		WithPollInterval(2*time.Second),
		WithPollBatchSize(7),
		WithConcurrency(3),
		WithQueueSize(11),
	)

	if p.cfg.LockTTL != 5*time.Second {
		t.Fatalf("unexpected lock TTL: got %s", p.cfg.LockTTL)
	}
	if p.cfg.PollInterval != 2*time.Second {
		t.Fatalf("unexpected poll interval: got %s", p.cfg.PollInterval)
	}
	if p.cfg.PollBatchSize != 7 {
		t.Fatalf("unexpected poll batch size: got %d", p.cfg.PollBatchSize)
	}
	if p.cfg.Concurrency != 3 {
		t.Fatalf("unexpected concurrency: got %d", p.cfg.Concurrency)
	}
	if cap(p.queue) != 11 {
		t.Fatalf("unexpected queue size: got %d", cap(p.queue))
	}
}

func TestProcessByIDCompletesTaskOnSuccess(t *testing.T) {
	taskID := TaskID(uuid.New())
	storage := &fakeStorage{
		tryLockTask: Task{
			ID:          taskID,
			Type:        "email.send",
			Status:      TaskStatusRunning,
			Attempts:    1,
			MaxAttempts: 3,
		},
		tryLockOK: true,
	}
	p := New(storage)

	handlerCalled := make(chan struct{}, 1)
	if err := p.RegisterHandler("email.send", func(context.Context, Task) error {
		handlerCalled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("RegisterHandler returned error: %v", err)
	}

	p.processByID(context.Background(), taskID)

	select {
	case <-handlerCalled:
	default:
		t.Fatal("expected handler to be called")
	}

	if len(storage.completed) != 1 || storage.completed[0] != taskID {
		t.Fatalf("expected task to be completed, got %+v", storage)
	}
	if len(storage.retried) != 0 || len(storage.failed) != 0 {
		t.Fatalf("expected no retry/fail, got %+v", storage)
	}
}

func TestExecuteRetriesTaskOnTransientError(t *testing.T) {
	taskID := TaskID(uuid.New())
	storage := &fakeStorage{}
	p := New(storage)

	if err := p.RegisterHandler("email.send", func(context.Context, Task) error {
		return errors.New("temporary")
	}); err != nil {
		t.Fatalf("RegisterHandler returned error: %v", err)
	}

	err := p.execute(context.Background(), Task{
		ID:          taskID,
		Type:        "email.send",
		Status:      TaskStatusRunning,
		Attempts:    1,
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("execute returned error: %v", err)
	}

	if len(storage.retried) != 1 || storage.retried[0].id != taskID {
		t.Fatalf("expected Retry to be called, got %+v", storage.retried)
	}
	if storage.retried[0].reason != "temporary" {
		t.Fatalf("unexpected retry reason: %q", storage.retried[0].reason)
	}
}

func TestExecuteFailsTaskAfterMaxAttempts(t *testing.T) {
	taskID := TaskID(uuid.New())
	storage := &fakeStorage{}
	p := New(storage)

	if err := p.RegisterHandler("email.send", func(context.Context, Task) error {
		return errors.New("permanent")
	}); err != nil {
		t.Fatalf("RegisterHandler returned error: %v", err)
	}

	err := p.execute(context.Background(), Task{
		ID:          taskID,
		Type:        "email.send",
		Status:      TaskStatusRunning,
		Attempts:    3,
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("execute returned error: %v", err)
	}

	if len(storage.failed) != 1 || storage.failed[0].id != taskID {
		t.Fatalf("expected Fail to be called, got %+v", storage.failed)
	}
	if storage.failed[0].reason != "permanent" {
		t.Fatalf("unexpected fail reason: %q", storage.failed[0].reason)
	}
}

func TestSubmitPersistsTaskAndQueuesID(t *testing.T) {
	taskID := TaskID(uuid.New())
	storage := &fakeStorage{
		createTask: Task{ID: taskID},
	}
	p := New(storage, WithQueueSize(1), WithConcurrency(1))

	if err := p.Submit(context.Background(), Task{Type: "email.send"}); err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	if len(storage.created) != 1 {
		t.Fatalf("expected one created task, got %d", len(storage.created))
	}

	select {
	case queuedID := <-p.queue:
		if queuedID != taskID {
			t.Fatalf("unexpected queued ID: got %v want %v", queuedID, taskID)
		}
	default:
		t.Fatal("expected task ID to be queued")
	}
}

type fakeStorage struct {
	mu sync.Mutex

	createTask  Task
	tryLockTask Task
	tryLockOK   bool
	tryLockErr  error

	created   []Task
	completed []TaskID
	retried   []taskError
	failed    []taskError
}

type taskError struct {
	id     TaskID
	reason string
}

func (s *fakeStorage) Create(_ context.Context, task Task) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.created = append(s.created, task)
	if s.createTask.ID != TaskID(uuid.Nil) {
		return s.createTask, nil
	}

	task.ID = TaskID(uuid.New())
	return task, nil
}

func (s *fakeStorage) TryLock(_ context.Context, _ TaskID, _ time.Duration) (Task, bool, error) {
	return s.tryLockTask, s.tryLockOK, s.tryLockErr
}

func (s *fakeStorage) FetchAndLock(_ context.Context, _ int, _ time.Duration) ([]Task, error) {
	return nil, nil
}

func (s *fakeStorage) Complete(_ context.Context, taskID TaskID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.completed = append(s.completed, taskID)
	return nil
}

func (s *fakeStorage) Retry(_ context.Context, taskID TaskID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retried = append(s.retried, taskError{id: taskID, reason: reason})
	return nil
}

func (s *fakeStorage) Fail(_ context.Context, taskID TaskID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failed = append(s.failed, taskError{id: taskID, reason: reason})
	return nil
}
