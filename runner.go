package pipeline

import (
	"context"
	"github.com/artyomturkin/go-stream"
	"sync"
)

// Runner running pipeline
type Runner interface {
	Done() <-chan struct{}
}

type runner struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	donech chan struct{}

	first Task

	name  string
	strm  stream.Consumer
	getID func(interface{}) string
}

func newRunner(ctx context.Context) *runner {
	return &runner{
		ctx:    ctx,
		donech: make(chan struct{}),
		wg:     &sync.WaitGroup{},
	}
}

func (r *runner) Done() <-chan struct{} {
	return r.donech
}

func (r *runner) Run() {
	msgs := r.strm.Messages()

	defer func() {
		r.wg.Wait()
		close(r.donech)
	}()

	for {
		select {
		case <-r.ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			ctx := context.WithValue(msg.Context, IDKey, r.getID(msg.Data))
			r.wg.Add(1)
			go r.handle(ctx, msg.Data)
		}
	}
}

func (r *runner) handle(ctx context.Context, m interface{}) {
	defer r.wg.Done()

	if r.first != nil {
		err := r.first.Execute(ctx, m)

		if err != nil {
			r.strm.Nack(ctx)
		}

	}

	r.strm.Ack(ctx)
}
