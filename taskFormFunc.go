package pipeline

import (
	"context"
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

	return ExecTasks(ctx, f.next, res)
}
