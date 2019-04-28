package pipeline_test

import (
	"context"
	"github.com/artyomturkin/go-pipeline"
	"testing"
	"time"
)

func TestBatch(t *testing.T) {
	strm := getStringStream()
	count, countMsgs := getCounterTask()

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

	if *count != 1 {
		t.Errorf("Wrong count. Want 1, got %d", count)
	}
}
