package tracer

import (
	"strings"
	"sync/atomic"
)

type concentrator struct {
	In chan *span

	tracer  *tracer
	stop    chan struct{}
	stopped uint32 // atomic bool
}

func newConcentrator(t *tracer) *concentrator {
	return &concentrator{
		In:     make(chan *span, 1000),
		tracer: t,
		stop:   make(chan struct{}),
	}
}

func (c *concentrator) Start() {
	defer close(c.stop)
	for {
		select {
		case in := <-c.In:
			_ = in
			// TODO
		case <-c.stop:
			atomic.AddUint32(&c.stopped, 1)
			return
		}
	}
}

func (c *concentrator) Stop() {
	if atomic.LoadUint32(&c.stopped) > 0 {
		return
	}
	c.stop <- struct{}{}
	<-c.stop
}

type aggregation struct {
	Env        string
	Resource   string
	Service    string
	Hostname   string
	StatusCode string
	Version    string
	Synthetics bool
}

func aggregationFromSpan(s *span, env string) aggregation {
	return aggregation{
		Env:        env,
		Resource:   s.Resource,
		Service:    s.Service,
		Hostname:   s.Meta[keyHostname],
		StatusCode: s.Meta["http.status_code"],
		Version:    s.Meta["version"],
		Synthetics: strings.HasPrefix(s.Meta[keyOrigin], "synthetics"),
	}
}
