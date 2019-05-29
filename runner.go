package pipeline

import (
	"context"
	"fmt"
	"github.com/artyomturkin/go-stream"
	"sync"
)

// Runner running pipeline
type Runner interface {
	Done() <-chan struct{}
	Errors() <-chan error
}

type runner struct {
	sync.Mutex

	ctx    context.Context
	wg     *sync.WaitGroup
	donech chan struct{}

	first []Task

	name  string
	strm  stream.Consumer
	getID func(interface{}) string
	errCh chan error
	errChs []chan error
}

func newRunner(ctx context.Context) *runner {
	return &runner{
		ctx:    ctx,
		donech: make(chan struct{}),
		wg:     &sync.WaitGroup{},
		errCh: make(chan error),
	}
}

func (r *runner) Done() <-chan struct{} {
	return r.donech
}

func (r *runner) Errors() <-chan error {
	ch := make(chan error)

	r.Lock()
	defer r.Unlock()

	r.errChs = append(r.errChs, ch)
	return ch
}

func (r *runner) Run() {
	msgs := r.strm.Messages()

	defer func() {
		r.wg.Wait()
		close(r.donech)
		close(r.errCh)
	}()

	go func() {
		wg := &sync.WaitGroup{}

		for {
			err, ok := <- r.errCh
			r.Lock()

			if !ok {
				wg.Wait()
				for _, ch := range r.errChs {
					close(ch)
				}
				r.Unlock()
				return
			}

			for _, ch := range r.errChs {
				wg.Add(1)
				go func(ch chan error){
					ch<-err
					wg.Done()
				}(ch)
			}
			r.Unlock()
		}
	}()

	for {
		select {
		case <-r.ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			ctx := context.WithValue(msg.Context, NameKey, r.name)
			ctx = context.WithValue(ctx, IDKey, r.getID(msg.Data))
			r.wg.Add(1)
			go r.handle(ctx, msg.Data)
		}
	}
}

func (r *runner) handle(ctx context.Context, m interface{}) {
	defer r.wg.Done()

	if r.first != nil {
		err := ExecTasks(ctx, r.first, m)

		if err != nil {
			r.strm.Nack(ctx)

			r.errCh<-err

			return
		}
	}

	r.strm.Ack(ctx)
}

// ExecTasks helper func to execute a group of tasks
func ExecTasks(ctx context.Context, tasks []Task, i interface{}) error {
	errs := []error{}
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for _, t := range tasks {
		wg.Add(1)

		go func(t Task) {
			defer wg.Done()

			err := t.Execute(ctx, i)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()

				errs = append(errs, err)
			}
		}(t)
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("Continuation tasks failed: %v", errs)
	}

	return nil
}
