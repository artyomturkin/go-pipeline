package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// Task accept messsage and context, executes it's logic and returns an error if failed
type Task interface {
	Name() string
	AddNext(Task)
	Execute(context.Context, interface{}) error
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
