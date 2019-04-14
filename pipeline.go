package stream

import (
	"context"
	"fmt"
	"sync"
)

// NewPipeline create new pipeline
func NewPipeline(name string) *PipelineBuilder {
	return &PipelineBuilder{
		name:      name,
		steps:     []PipelineStep{},
		maxErrors: -1,
	}
}

// PipelineStep a step in stream pipeline.
// Accepts a message and returns a transformed message or an error
// Return a nil message if no further processing is required, original message will be acknowledged
// Return an error if further processing is not possible at the moment, original message will not be acknowledged and would be available for processing later.
type PipelineStep func(context.Context, *Message) (*Message, error)

// PipelineBuilder used to configure and create pipeline
type PipelineBuilder struct {
	name   string
	source Stream
	steps  []PipelineStep

	maxErrors int
}

// MaxErrors set maximum amount of processing errors before terminating.
// Less then 0, to do nothing (default)
// Set 0 to terminate on first error
func (p *PipelineBuilder) MaxErrors(n int) *PipelineBuilder {
	p.maxErrors = n
	return p
}

// From set a source stream
func (p *PipelineBuilder) From(source Stream) *PipelineBuilder {
	p.source = source
	return p
}

// To set an output stream, should be the last step in a pipeline
func (p *PipelineBuilder) To(pr Stream) *PipelineBuilder {
	stream := pr.GetProducer(p.name)
	p.steps = append(p.steps,
		func(ctx context.Context, m *Message) (*Message, error) {
			return nil, stream.Publish(ctx, m)
		})

	return p
}

// Do execute provided steps in a pipeline
func (p *PipelineBuilder) Do(steps ...PipelineStep) *PipelineBuilder {
	p.steps = append(p.steps, steps...)
	return p
}

// Build and start pipeline
func (p *PipelineBuilder) Build(ctx context.Context) *Pipeline {
	pipe := &Pipeline{
		name:      p.name,
		source:    p.source.GetConsumer(p.name),
		steps:     p.steps,
		maxErrors: p.maxErrors,
	}

	go pipe.start(ctx)

	return pipe
}

// Pipeline stream processing pipeline
type Pipeline struct {
	sync.Mutex
	wg *sync.WaitGroup

	name   string
	source Consumer
	steps  []PipelineStep

	maxErrors int

	doneCh          []chan error
	terminated      bool
	terminatedError error
}

func (p *Pipeline) done(err error) {
	p.Lock()
	defer p.Unlock()

	if p.terminated {
		return
	}

	p.terminated = true
	p.terminatedError = err

	go func() {
		p.wg.Wait()
		p.Lock()
		defer p.Unlock()

		for _, ch := range p.doneCh {
			ch <- err
		}
	}()
}

func (p *Pipeline) start(ctx context.Context) {
	p.Lock()
	numErr := 0
	p.wg = &sync.WaitGroup{}
	p.Unlock()

	for {
		// if pipeline is already terminated, stop
		if p.terminated {
			break
		}

		origM, err := p.source.Read(ctx)

		// if failed to read from source, terminate pipeline, source is broken
		if err != nil {
			p.done(ErrorReadingMessage{internal: err})
			break
		}

		p.wg.Add(1)

		// process messages in another goroutine
		go func() {
			m := origM
			var err error

			for _, step := range p.steps {
				// if context is done, stop processing
				if err = ctx.Err(); err != nil {
					break
				}

				m, err = step(ctx, m)

				// if error in step or no message, stop processing
				if err != nil || m == nil {
					break
				}
			}

			// if failed to process message, nack it, otherwise ack it
			if err != nil {
				err = p.source.Nack(ctx, origM)

				// if failed to nack, terminate pipeline, source is probably broken
				if err != nil {
					p.done(ErrorNackMsg{internal: err})
				}

				numErr++
				// if more then max allowed, terminate
				if p.maxErrors >= 0 && numErr > p.maxErrors {
					p.done(ErrorMaxErrorsExceeded{maxAllowed: p.maxErrors, counter: numErr})
				}
			} else {
				err = p.source.Ack(ctx, origM)

				// if failed to ack, terminate pipeline, source is probably broken
				if err != nil {
					p.done(ErrorAckMsg{internal: err})
				}
			}

			p.wg.Done()
		}()
	}
}

// Done returns channel that will produce either a nil, if pipeline finished successfully, or return an error, which caused the pipeline to terminate.
func (p *Pipeline) Done() <-chan error {
	p.Lock()
	defer p.Unlock()

	ch := make(chan error, 1)

	if p.doneCh == nil {
		p.doneCh = []chan error{ch}
	} else {
		p.doneCh = append(p.doneCh, ch)
	}

	if p.terminated {
		ch <- p.terminatedError
	}
	return ch
}

// ErrorAckMsg failed to perform ack on a message
type ErrorAckMsg struct {
	internal error
}

// Error return error text
func (e ErrorAckMsg) Error() string {
	return fmt.Sprintf("failed to ack message: %v", e.internal)
}

// Internal get internal error
func (e ErrorAckMsg) Internal() error {
	return e.internal
}

// ErrorNackMsg failed to perform ack on a message
type ErrorNackMsg struct {
	internal error
}

// Error return error text
func (e ErrorNackMsg) Error() string {
	return fmt.Sprintf("failed to nack message: %v", e.internal)
}

// Internal get internal error
func (e ErrorNackMsg) Internal() error {
	return e.internal
}

// ErrorMaxErrorsExceeded failed to perform ack on a message
type ErrorMaxErrorsExceeded struct {
	counter    int
	maxAllowed int
}

// Error return error text
func (e ErrorMaxErrorsExceeded) Error() string {
	return fmt.Sprintf("max errors allowed exceeded: %d/%d", e.counter, e.maxAllowed)
}

// ErrorReadingMessage failed to perform ack on a message
type ErrorReadingMessage struct {
	internal error
}

// Error return error text
func (e ErrorReadingMessage) Error() string {
	return fmt.Sprintf("failed to read message: %v", e.internal)
}

// Internal get internal error
func (e ErrorReadingMessage) Internal() error {
	return e.internal
}
