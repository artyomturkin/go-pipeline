package pipeline_test

import (
	"context"
	"github.com/artyomturkin/go-pipeline"
	"testing"
)

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
	msgs, saveMsgs := getSaveMessagesTask()
	count, countMsgs := getCounterTask()

	r := pipeline.New("empty-test").
		From(strm, getIDFromString).
		Then(saveMsgs).
		Then(countMsgs).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if len(*msgs) != 10 {
		t.Errorf("Wrong number of msgs. Want 10, got %d", len(*msgs))
	}

	if *count != 10 {
		t.Errorf("Wrong count. Want 10, got %d", *count)
	}
}

func TestMultipleTasksFromFuncError(t *testing.T) {
	strm := getStringStream()
	msgs, saveMsgs := getSaveMessagesTask()
	errTask := getErrorTask()
	count, countMsgs := getCounterTask()

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

	if len(*msgs) != 10 {
		t.Errorf("Wrong number of msgs. Want 10, got %d", len(*msgs))
	}

	if *count != 0 {
		t.Errorf("Wrong count. Want 0, got %d", *count)
	}
}

func TestAfterTasksFromFunc(t *testing.T) {
	strm := getStringStream()
	msgs, saveMsgs := getSaveMessagesTask()
	count, countMsgs := getCounterTask()
	count2, count2Msgs := getCounterTask()

	r := pipeline.New("empty-test").
		From(strm, getIDFromString).
		Then(saveMsgs).
		Then(countMsgs).
		After(pipeline.Input, count2Msgs).
		Start(context.TODO())

	<-r.Done()

	if len(strm.Acks) != 10 {
		t.Errorf("Wrong number of Acks. Want 10, got %d", len(strm.Acks))
	}

	if len(*msgs) != 10 {
		t.Errorf("Wrong number of msgs. Want 10, got %d", len(*msgs))
	}

	if *count != 10 {
		t.Errorf("Wrong count. Want 10, got %d", *count)
	}

	if *count2 != 10 {
		t.Errorf("Wrong second count. Want 10, got %d", *count)
	}
}
