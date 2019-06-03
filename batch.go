package pipeline

import (
	"context"
	"strings"
	"time"
)

// Batch batches incoming messages and flushes them when specified size is reached or specified duration has elapsed
func Batch(name string, size int, interval time.Duration) Task {
	b := &batch{
		name:    name,
		size:    size,
		flush:   interval,
		inputCh: make(chan batchInput),
	}


	return b
}

type batch struct {
	name string
	next []Task
	ctx  context.Context

	size  int
	flush time.Duration

	inputCh chan batchInput
}

func (b *batch) SetContext(ctx context.Context) {
	b.ctx = ctx
	go b.run()
}

func (b *batch) Name() string {
	return b.name
}

func (b *batch) AddNext(t Task) {
	b.next = append(b.next, t)
}

func (b *batch) Execute(ctx context.Context, i interface{}) error {
	resCh := make(chan error, 1)

	b.inputCh <- batchInput{resCh: resCh, ctx: ctx, data: i}

	return <-resCh
}

type batchInput struct {
	resCh chan error
	ctx   context.Context
	data  interface{}
}

func (b *batch) run() {
	flush := time.NewTicker(b.flush)
	bis := make([]*batchInput, b.size)
	counter := 0

	f := func() {
		msg := []interface{}{}
		ids := []string{}
		for _, bi := range bis {
			if bi != nil {
				ids = append(ids, bi.ctx.Value(IDKey).(string))
				msg = append(msg, bi.data)
			}
		}

		if len(msg) > 0 {
			ctx := context.WithValue(b.ctx, IDKey, strings.Join(ids, "|"))
			err := ExecTasks(ctx, b.next, msg)

			for i, bi := range bis {
				if bi != nil {
					bi.resCh <- err
					close(bi.resCh)
					bis[i] = nil
				}
			}
			counter = 0
		}
	}

	for {
		select {
		case <-b.ctx.Done():
			flush.Stop()
			f()
			return
		case <-flush.C:
			f()
		case m := <-b.inputCh:
			bis[counter] = &m
			counter++

			if counter == b.size {
				f()
			}
		}
	}

}
