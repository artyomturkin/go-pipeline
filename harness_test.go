package pipeline_test

import (
	"context"
	"fmt"
	"github.com/artyomturkin/go-pipeline"
	"github.com/artyomturkin/go-stream"
	"sync"
	"sync/atomic"
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

func getSaveMessagesTask() (*[]string, pipeline.Task) {
	msgs := []string{}
	mu := &sync.Mutex{}
	saveMsgs := pipeline.TaskFromFunc("save-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			mu.Lock()
			defer mu.Unlock()

			msgs = append(msgs, msg.(string))

			return msg, nil
		})
	return &msgs, saveMsgs
}

func getCounterTask() (*int32, pipeline.Task) {
	var count int32
	countMsgs := pipeline.TaskFromFunc("count-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			atomic.AddInt32(&count, 1)

			return msg, nil
		})
	return &count, countMsgs
}

func getErrorTask() pipeline.Task {
	return pipeline.TaskFromFunc("error",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			return nil, fmt.Errorf("expected error")
		})
}
