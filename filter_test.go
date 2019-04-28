package pipeline_test

import (
	"context"
	"github.com/artyomturkin/go-pipeline"
	"sync/atomic"
	"testing"
)

func TestFilter(t *testing.T) {
	strm := getStringStream()

	var count int32
	countMsgs := pipeline.TaskFromFunc("count-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			atomic.AddInt32(&count, 1)

			return msg, nil
		})

	filter := pipeline.Filter("filter-message-0", func(_ context.Context, i interface{}) bool {
		return i.(string) == "message-0"
	})

	r := pipeline.New("filter-test").
		From(strm, getIDFromString).
		Then(filter).
		Then(countMsgs).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if count != 9 {
		t.Errorf("Wrong count. Want 9, got %d", count)
	}
}

func TestSelect(t *testing.T) {
	strm := getStringStream()

	var count int32
	countMsgs := pipeline.TaskFromFunc("count-msgs",
		func(ctx context.Context, msg interface{}) (interface{}, error) {
			atomic.AddInt32(&count, 1)

			return msg, nil
		})

	filter := pipeline.Select("filter-message-0", func(_ context.Context, i interface{}) bool {
		return i.(string) == "message-0"
	})

	r := pipeline.New("filter-test").
		From(strm, getIDFromString).
		Then(filter).
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
