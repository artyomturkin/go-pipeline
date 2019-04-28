package pipeline_test

import (
	"context"
	"fmt"
	"github.com/artyomturkin/go-pipeline"
	"github.com/artyomturkin/go-stream"
	"sync"
	"sync/atomic"
	"testing"
)

func getStringStream() *stream.InmemStream {
	return &stream.InmemStream{
		Messages: []interface{}{
			"message-0",
			"message-1",
			"message-2",
			"message-3",
			"message-4",
			"message-5",
			"message-6",
			"message-7",
			"message-8",
			"message-9",
		},
	}
}

func getIDFromString(i interface{}) string {
	return i.(string)
}

func TestNoTasks(t *testing.T) {
	strm := getStringStream()

	r := pipeline.New("empty-test").From(strm, getIDFromString).Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}
}

func TestMultipleTasksFromFunc(t *testing.T) {
	strm := getStringStream()

	msgs := []string{}
	mu := &sync.Mutex{}
	saveMsgs := pipeline.TaskFromFunc("save-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			mu.Lock()
			defer mu.Unlock()

			msgs = append(msgs, msg.(string))

			return msg, nil
		})

	var count int32
	countMsgs := pipeline.TaskFromFunc("count-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			atomic.AddInt32(&count, 1)

			return msg, nil
		})

	r := pipeline.New("empty-test").
		From(strm, getIDFromString).
		Then(saveMsgs).
		Then(countMsgs).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if len(msgs) != 10 {
		t.Errorf("Wrong number of msgs. Want 10, got %d", len(strm.Acks))
	}

	if count != 10 {
		t.Errorf("Wrong count. Want 10, got %d", count)
	}
}

func TestMultipleTasksFromFuncError(t *testing.T) {
	strm := getStringStream()

	msgs := []string{}
	mu := &sync.Mutex{}
	saveMsgs := pipeline.TaskFromFunc("save-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			mu.Lock()
			defer mu.Unlock()

			msgs = append(msgs, msg.(string))

			return msg, nil
		})

	errTask := pipeline.TaskFromFunc("error",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			return nil, fmt.Errorf("expected error")
		})

	var count int32
	countMsgs := pipeline.TaskFromFunc("count-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			atomic.AddInt32(&count, 1)

			return msg, nil
		})

	r := pipeline.New("empty-test").
		From(strm, getIDFromString).
		Then(saveMsgs).
		Then(errTask).
		Then(countMsgs).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Nacks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if len(msgs) != 10 {
		t.Errorf("Wrong number of msgs. Want 10, got %d", len(strm.Acks))
	}

	if count != 0 {
		t.Errorf("Wrong count. Want 0, got %d", count)
	}
}
