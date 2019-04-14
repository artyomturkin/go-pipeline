package stream_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	stream "github.com/artyomturkin/go-stream"
)

func forward(_ context.Context, m *stream.Message) (*stream.Message, error) {
	return m, nil
}

var errorExpected = errors.New("expected error")

func forwardAndFailAfter(n int) func(_ context.Context, m *stream.Message) (*stream.Message, error) {
	mu := &sync.Mutex{}
	count := 0

	return func(_ context.Context, m *stream.Message) (*stream.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		count++

		if count > n {
			return nil, errorExpected
		}

		return m, nil
	}
}

func TestPipeline_Success(t *testing.T) {
	// setup test data
	msgs := []*stream.Message{}
	pubs := map[*stream.Message]error{}
	tmsgs := []TestMessage{}

	for index := 0; index < 10; index++ {
		msg := &stream.Message{
			Source: "test",
			Type:   "int",
			ID:     strconv.Itoa(index),
			Time:   time.Now(),
			Data:   index,
		}

		pubs[msg] = nil
		tmsgs = append(tmsgs, TestMessage{Message: msg, Error: nil})
		msgs = append(msgs, msg)
	}

	// create test streams
	source := &TestStream{Messages: tmsgs}
	output := &TestStream{PubResults: pubs}

	// create pipeline
	p := stream.NewPipeline("test").From(source).Do(forward).To(output).Build(context.TODO())

	// wait for timeout or completion
	select {
	case <-time.After(time.Second):
		t.Fatalf("test timed out")
	case err := <-p.Done():
		if e, ok := err.(stream.ErrorReadingMessage); !ok || (ok && e.Internal() != ErrorNoNewMessages) {
			t.Fatalf("got unexpected error: %v", err)
		}
	}

	err := <-p.Done() // for coverage report call twice
	if e, ok := err.(stream.ErrorReadingMessage); !ok || (ok && e.Internal() != ErrorNoNewMessages) {
		t.Fatalf("got unexpected error second time: %v", err)
	}

	// perform assertions
	if len(msgs) != len(output.PublishedMessages) {
		t.Fatalf("published message count does not match expected. got %d/%d", len(output.PublishedMessages), len(msgs))
	}
}

func TestPipeline_FailStep(t *testing.T) {
	// setup test data
	msgs := []*stream.Message{}
	pubs := map[*stream.Message]error{}
	tmsgs := []TestMessage{}

	for index := 0; index < 10; index++ {
		msg := &stream.Message{
			Source: "test",
			Type:   "int",
			ID:     strconv.Itoa(index),
			Time:   time.Now(),
			Data:   index,
		}

		pubs[msg] = nil
		tmsgs = append(tmsgs, TestMessage{Message: msg, Error: nil})
		msgs = append(msgs, msg)
	}

	// create test streams
	source := &TestStream{Messages: tmsgs, BlockAfterAllMessagesRead: true}
	output := &TestStream{PubResults: pubs}

	// create pipeline
	p := stream.NewPipeline("test").
		From(source).
		Do(forwardAndFailAfter(5)).
		To(output).
		MaxErrors(0).
		Build(context.TODO())

	// wait for timeout or completion
	select {
	case <-time.After(2 * time.Second):
		t.Fatalf("test timed out")
	case err := <-p.Done():
		if _, ok := err.(stream.ErrorMaxErrorsExceeded); !ok {
			t.Fatalf("got unexpected error: %v", err)
		}
	}

	// perform assertions
	if 5 != len(output.PublishedMessages) {
		t.Fatalf("published message count does not match expected. got %d/%d", len(output.PublishedMessages), 5)
	}
}

func TestPipeline_FailAck(t *testing.T) {
	// setup test data
	msgs := []*stream.Message{}
	pubs := map[*stream.Message]error{}
	tmsgs := []TestMessage{}

	for index := 0; index < 10; index++ {
		msg := &stream.Message{
			Source: "test",
			Type:   "int",
			ID:     strconv.Itoa(index),
			Time:   time.Now(),
			Data:   index,
		}

		pubs[msg] = nil
		tmsgs = append(tmsgs, TestMessage{Message: msg, Error: nil})
		msgs = append(msgs, msg)
	}

	// create test streams
	source := &TestStream{
		Messages: tmsgs,
		AckResults: map[*stream.Message]error{
			msgs[5]: errorExpected,
		},
		BlockAfterAllMessagesRead: true,
	}
	output := &TestStream{PubResults: pubs}

	// create pipeline
	p := stream.NewPipeline("test").From(source).Do(forward).To(output).Build(context.TODO())

	// wait for timeout or completion
	select {
	case <-time.After(time.Second):
		t.Fatalf("test timed out")
	case err := <-p.Done():
		if _, ok := err.(stream.ErrorAckMsg); !ok {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
}

func TestPipeline_FailNack(t *testing.T) {
	// setup test data
	msgs := []*stream.Message{}
	pubs := map[*stream.Message]error{}
	tmsgs := []TestMessage{}

	for index := 0; index < 10; index++ {
		msg := &stream.Message{
			Source: "test",
			Type:   "int",
			ID:     strconv.Itoa(index),
			Time:   time.Now(),
			Data:   index,
		}

		pubs[msg] = nil
		tmsgs = append(tmsgs, TestMessage{Message: msg, Error: nil})
		msgs = append(msgs, msg)
	}

	// create test streams
	source := &TestStream{
		Messages:                  tmsgs,
		BlockAfterAllMessagesRead: true,
		AckResults: map[*stream.Message]error{
			msgs[5]: errorExpected,
		},
	}
	output := &TestStream{PubResults: pubs}

	// create pipeline
	p := stream.NewPipeline("test").
		From(source).
		Do(forwardAndFailAfter(0)).
		To(output).
		Build(context.TODO())

	// wait for timeout or completion
	select {
	case <-time.After(2 * time.Second):
		t.Fatalf("test timed out")
	case err := <-p.Done():
		if _, ok := err.(stream.ErrorNackMsg); !ok {
			t.Errorf("got unexpected error: %v", err)
		}
	}
}

func TestPipeline_CancelContext(t *testing.T) {
	// setup test data
	msgs := []*stream.Message{}
	pubs := map[*stream.Message]error{}
	tmsgs := []TestMessage{}

	for index := 0; index < 10; index++ {
		msg := &stream.Message{
			Source: "test",
			Type:   "int",
			ID:     strconv.Itoa(index),
			Time:   time.Now(),
			Data:   index,
		}

		pubs[msg] = nil
		tmsgs = append(tmsgs, TestMessage{Message: msg, Error: nil})
		msgs = append(msgs, msg)
	}

	// create test streams
	source := &TestStream{Messages: tmsgs, BlockAfterAllMessagesRead: true}
	output := &TestStream{PubResults: pubs}

	// create pipeline and cancel it
	ctx, cancel := context.WithCancel(context.TODO())
	p := stream.NewPipeline("test").From(source).Do(forward).To(output).Build(ctx)
	cancel()

	// wait for timeout or completion
	select {
	case <-time.After(time.Second):
		t.Fatalf("test timed out")
	case err := <-p.Done():
		if e, ok := err.(stream.ErrorReadingMessage); !ok || (ok && e.Internal() != ErrorStreamCanceled) {
			t.Fatalf("got unexpected error: %v", err)
		}
	}
}
