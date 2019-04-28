package pipeline

import (
	"context"
	"github.com/artyomturkin/go-stream"
)

// Output write message to stream
func Output(name string, str stream.Stream) Task {
	return &output{
		name: name,
		str:  str,
	}
}

type output struct {
	name string
	next []Task
	ctx  context.Context
	str  stream.Stream

	producer stream.Producer
}

func (f *output) Name() string {
	return f.name
}

func (f *output) AddNext(t Task) {
	f.next = append(f.next, t)
}

func (f *output) SetContext(ctx context.Context) {
	f.ctx = ctx
	f.producer = f.str.GetProducer(f.ctx, f.name)
}

func (f *output) Execute(ctx context.Context, i interface{}) error {
	err := f.producer.Publish(ctx, i)

	if err != nil {
		return err
	}

	return ExecTasks(ctx, f.next, i)
}
