package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// TaskFromFunc creates a task from supplied func and continuation tasks
func TaskFromFunc(name string, f func(context.Context, interface{}) (interface{}, error)) Task {
	return &taskFromFunc{
		name: name,
		f:    f,
		next: []Task{},
	}
}

type taskFromFunc struct {
	name string
	next []Task
	f    func(context.Context, interface{}) (interface{}, error)
}

func (f *taskFromFunc) Name() string {
	return f.name
}

func (f *taskFromFunc) AddNext(t Task) {
	f.next = append(f.next, t)
}

func (f *taskFromFunc) Execute(ctx context.Context, i interface{}) error {
	res, err := f.f(ctx, i)

	if err != nil {
		return err
	}

	errs := []error{}
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for _, t := range f.next {
		wg.Add(1)

		go func(t Task) {
			defer wg.Done()

			err := t.Execute(ctx, res)
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
