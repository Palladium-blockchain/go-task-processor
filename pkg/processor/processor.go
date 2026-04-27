package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Options struct {
	LockTTL       time.Duration
	PollInterval  time.Duration
	PollBatchSize int
	Concurrency   int
	QueueSize     int
}

type Processor struct {
	options  Options
	storage  TaskStorage
	queue    chan TaskID
	handlers map[string]TaskHandler
	mu       sync.Mutex
}

func NewProcessor(storage TaskStorage, options Options) *Processor {
	if options.LockTTL <= 0 {
		options.LockTTL = 30 * time.Second
	}
	if options.PollInterval <= 0 {
		options.PollInterval = time.Second
	}
	if options.PollBatchSize <= 0 {
		options.PollBatchSize = 100
	}
	if options.Concurrency <= 0 {
		options.Concurrency = 10
	}
	if options.QueueSize <= 0 {
		options.QueueSize = options.Concurrency
	}

	return &Processor{
		options:  options,
		storage:  storage,
		queue:    make(chan TaskID, options.QueueSize),
		handlers: map[string]TaskHandler{},
	}
}

func (p *Processor) Submit(ctx context.Context, task Task) error {
	saved, err := p.storage.Create(ctx, task)
	if err != nil {
		return err
	}

	select {
	case p.queue <- saved.ID:
	default:
		// The poller will pick the task up from durable storage.
	}

	return nil
}

func (p *Processor) RegisterHandler(taskType string, handler TaskHandler) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.handlers[taskType]; ok {
		return fmt.Errorf("task handler %s already registered", taskType)
	}

	p.handlers[taskType] = handler
	return nil
}

func (p *Processor) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < p.options.Concurrency; i++ {
		g.Go(func() error {
			p.worker(ctx)
			return nil
		})
	}

	g.Go(func() error {
		p.poller(ctx)
		return nil
	})

	return g.Wait()
}

func (p *Processor) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case taskID := <-p.queue:
			p.processByID(ctx, taskID)
		}
	}
}

func (p *Processor) processByID(ctx context.Context, taskID TaskID) {
	task, ok, err := p.storage.TryLock(ctx, taskID, p.options.LockTTL)
	if err != nil || !ok {
		return
	}

	_ = p.execute(ctx, task)
}

func (p *Processor) execute(ctx context.Context, task Task) error {
	handler, ok := p.handlers[task.Type]
	if !ok {
		return fmt.Errorf("task handler %s not registered", task.Type)
	}

	if err := handler(ctx, task); err != nil {
		if task.Attempts >= task.MaxAttempts {
			return p.storage.Fail(ctx, task.ID, err.Error())
		}

		return p.storage.Retry(ctx, task.ID, err.Error())
	}

	return p.storage.Complete(ctx, task.ID)
}

func (p *Processor) poller(ctx context.Context) {
	ticker := time.NewTicker(p.options.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := p.storage.FetchAndLock(ctx, p.options.PollBatchSize, p.options.LockTTL)
			if err != nil {
				continue
			}

			for _, task := range tasks {
				_ = p.execute(ctx, task)
			}
		}
	}
}
