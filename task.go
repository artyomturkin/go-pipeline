package pipeline

import (
	"context"
)

// Task accept messsage and context, executes it's logic and returns an error if failed
type Task interface {
	Name() string
	AddNext(Task)
	Execute(context.Context, interface{}) error
	SetContext(context.Context)
}
