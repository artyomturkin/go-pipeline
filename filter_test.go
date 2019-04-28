package pipeline_test

import (
	"context"
	"github.com/artyomturkin/go-pipeline"
	"testing"
)

func TestFilter(t *testing.T) {
	strm := getStringStream()
	count, countMsgs := getCounterTask()

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

	if *count != 9 {
		t.Errorf("Wrong count. Want 9, got %d", *count)
	}
}

func TestSelect(t *testing.T) {
	strm := getStringStream()
	count, countMsgs := getCounterTask()

	slct := pipeline.Select("slct-message-0", func(_ context.Context, i interface{}) bool {
		return i.(string) == "message-0"
	})

	r := pipeline.New("slct-test").
		From(strm, getIDFromString).
		Then(slct).
		Then(countMsgs).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if *count != 1 {
		t.Errorf("Wrong count. Want 1, got %d", *count)
	}
}
