package processor

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type TaskID uuid.UUID

const (
	TaskStatusPending = iota
	TaskStatusRunning
	TaskStatusDone
	TaskStatusFailed
)

type Task struct {
	ID          TaskID
	Status      int
	Type        string
	Payload     []byte
	Attempts    int
	MaxAttempts int
	CreatedAt   time.Time
}

type TaskHandler func(ctx context.Context, task Task) error
