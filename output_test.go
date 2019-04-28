package pipeline_test

import (
	"context"
	"github.com/artyomturkin/go-pipeline"
	"github.com/artyomturkin/go-stream"
	"testing"
)

func TestOutput(t *testing.T) {
	strm := getStringStream()
	count, countMsgs := getCounterTask()
	outStream := &stream.InmemStream{}

	r := pipeline.New("filter-test").
		From(strm, getIDFromString).
		Then(countMsgs).
		Then(pipeline.Output("out", outStream)).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if *count != 10 {
		t.Errorf("Wrong count. Want 1, got %d", *count)
	}

	if len(outStream.Messages) != 10 {
		t.Errorf("Worng numbet of messages. Want 10, got %d\nValues: %v", len(outStream.Messages), outStream.Messages)
	}
}
