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
		t.Errorf("Worng number of messages. Want 10, got %d\nValues: %v", len(outStream.Messages), outStream.Messages)
	}
}

func TestErrors(t *testing.T) {
	strm := getStringStream()
	errStep := getErrorTask()

	r := pipeline.New("filter-test").
		From(strm, getIDFromString).
		Then(errStep).
		Start(context.TODO())

	
	errs := []error{}
	for err := range r.Errors() {
		errs = append(errs, err)
	}

	<-r.Done()

	if len(strm.Nacks) != 10 {
		t.Errorf("Wrong number of Nacks. Want 10, got %d", len(strm.Acks))
	}

	if len(errs) != 10 {
		t.Errorf("Worng number of errors. Want 10, got %d\nValues: %v", len(errs), errs)
	}
}
