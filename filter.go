package pipeline

import (
	"context"
)

// Filter drops matched messages
func Filter(name string, condition func(context.Context, interface{}) bool) Task {
	return &filter{
		name:      name,
		next:      []Task{},
		condition: condition,
		filter:    true,
	}
}

// Select drops unmatched messages
func Select(name string, condition func(context.Context, interface{}) bool) Task {
	return &filter{
		name:      name,
		next:      []Task{},
		condition: condition,
		filter:    false,
	}
}

type filter struct {
	name      string
	next      []Task
	condition func(context.Context, interface{}) bool
	filter    bool
}

func (f *filter) Name() string {
	return f.name
}

func (f *filter) AddNext(t Task) {
	f.next = append(f.next, t)
}

func (f *filter) Execute(ctx context.Context, i interface{}) error {
	res := f.condition(ctx, i)

	if (f.filter && res) || (!f.filter && !res) {
		return nil
	}

	return ExecTasks(ctx, f.next, i)
}
