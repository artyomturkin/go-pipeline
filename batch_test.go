package pipeline_test

import (
	"context"
	"github.com/artyomturkin/go-pipeline"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatch(t *testing.T) {
	strm := getStringStream()

	var count int32
	countMsgs := pipeline.TaskFromFunc("count-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			atomic.AddInt32(&count, 1)

			return msg, nil
		})

	batch := pipeline.Batch("batch", 10, 10*time.Second)

	r := pipeline.New("filter-test").
		From(strm, getIDFromString).
		Then(batch).
		Then(countMsgs).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if count != 1 {
		t.Errorf("Wrong count. Want 1, got %d", count)
	}
}
