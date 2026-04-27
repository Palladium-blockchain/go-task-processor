package processor

import (
	"context"
	"time"
)

type TaskStorage interface {
	Create(ctx context.Context, task Task) (Task, error)
	TryLock(ctx context.Context, taskID TaskID, lockTTL time.Duration) (Task, bool, error)
	FetchAndLock(ctx context.Context, limit int, lockTTL time.Duration) ([]Task, error)
	Complete(ctx context.Context, taskID TaskID) error
	Retry(ctx context.Context, taskID TaskID, reason string) error
	Fail(ctx context.Context, taskID TaskID, reason string) error
}
