package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Option func(*processorConfig)

type processorConfig struct {
	LockTTL       time.Duration
	PollInterval  time.Duration
	PollBatchSize int
	Concurrency   int
	QueueSize     int
}

type Processor struct {
	cfg      processorConfig
	storage  TaskStorage
	queue    chan TaskID
	handlers map[string]TaskHandler
	mu       sync.Mutex
}

func New(storage TaskStorage, opts ...Option) *Processor {
	cfg := processorConfig{
		LockTTL:       10 * time.Second,
		PollInterval:  time.Second,
		PollBatchSize: 10,
		Concurrency:   10,
		QueueSize:     20,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return &Processor{
		cfg:      cfg,
		storage:  storage,
		queue:    make(chan TaskID, cfg.QueueSize),
		handlers: map[string]TaskHandler{},
	}
}

func WithLockTTL(lockTTL time.Duration) Option {
	return func(options *processorConfig) {
		options.LockTTL = lockTTL
	}
}

func WithPollInterval(pollInterval time.Duration) Option {
	return func(options *processorConfig) {
		options.PollInterval = pollInterval
	}
}

func WithPollBatchSize(pollBatchSize int) Option {
	return func(options *processorConfig) {
		options.PollBatchSize = pollBatchSize
	}
}

func WithConcurrency(concurrency int) Option {
	return func(options *processorConfig) {
		options.Concurrency = concurrency
	}
}

func WithQueueSize(queueSize int) Option {
	return func(options *processorConfig) {
		options.QueueSize = queueSize
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

	for i := 0; i < p.cfg.Concurrency; i++ {
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
	task, ok, err := p.storage.TryLock(ctx, taskID, p.cfg.LockTTL)
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
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := p.storage.FetchAndLock(ctx, p.cfg.PollBatchSize, p.cfg.LockTTL)
			if err != nil {
				continue
			}

			for _, task := range tasks {
				_ = p.execute(ctx, task)
			}
		}
	}
}
