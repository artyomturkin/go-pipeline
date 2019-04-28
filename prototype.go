package pipeline

import (
	"context"
	"fmt"
	"github.com/artyomturkin/go-stream"
	"sync"
)

// Prototype pipeline builder
type Prototype interface {
	From(source stream.Stream, getID func(interface{}) string) Prototype
	To(output stream.Stream) Prototype
	Then(t Task) Prototype
	After(name string, t Task) Prototype

	Start(context.Context) Runner
}

// New start constructing new pipeline
func New(name string) Prototype {
	return &prototype{
		name:  name,
		tasks: map[string]Task{},
	}
}

type prototype struct {
	sync.Mutex

	name   string
	source stream.Stream
	getID  func(interface{}) string
	output stream.Stream

	last  Task
	first Task

	tasks map[string]Task
}

func (p *prototype) From(s stream.Stream, getID func(interface{}) string) Prototype {
	p.Lock()
	defer p.Unlock()

	p.source = s
	p.getID = getID
	return p
}

func (p *prototype) To(s stream.Stream) Prototype {
	p.Lock()
	defer p.Unlock()

	p.output = s
	return p
}

func (p *prototype) Start(ctx context.Context) Runner {
	p.Lock()
	defer p.Unlock()

	r := newRunner(ctx)

	r.strm = p.source.GetConsumer(ctx, p.name)
	r.getID = p.getID
	r.first = p.first

	for _, t := range p.tasks {
		t.SetContext(ctx)
	}

	go r.Run()
	return r
}

func (p *prototype) Then(t Task) Prototype {
	p.Lock()
	defer p.Unlock()

	if p.first == nil {
		p.first = t
	}

	p.tasks[t.Name()] = t

	if p.last != nil {
		p.last.AddNext(t)
	}

	p.last = t

	return p
}

func (p *prototype) After(name string, t Task) Prototype {
	p.Lock()
	defer p.Unlock()

	if p.first == nil {
		panic(fmt.Errorf("After can be used only when some other task exists"))
	}

	if l, ok := p.tasks[name]; ok {
		l.AddNext(t)
	} else {
		panic(fmt.Errorf("Task '%s' does not exist", name))
	}

	p.tasks[t.Name()] = t
	p.last = t

	return p
}
