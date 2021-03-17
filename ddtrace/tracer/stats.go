// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

//go:generate msgp -unexported -marshal=false -o=stats_msgp.go -tests=false

package tracer

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/tinylib/msgp/msgp"
)

type spanSummary struct {
	Start, Duration int64
	Name            string
	Resource        string
	Service         string
	Type            string
	Error           int32
	Hostname        string
	Synthetics      bool
	Env             string
	StatusCode      uint32
	Version         string
	TopLevel        bool
}

type concentrator struct {
	In chan *spanSummary

	mu      sync.Mutex
	buckets map[int64]*rawBucket // buckets used to aggregate stats per timestamp

	bufferLen int
	oldestTs  int64
	stop      chan struct{}
	stopped   uint32 // atomic bool
	cfg       *config
}

// defaultBufferLen represents the default buffer length; the number of bucket size
// units used by the concentrator.
const defaultBufferLen = 2

func newConcentrator(c *config) *concentrator {
	return &concentrator{
		In:        make(chan *spanSummary, 10000),
		bufferLen: defaultBufferLen,
		stop:      make(chan struct{}),
		oldestTs:  alignTs(time.Now().UnixNano()),
		buckets:   make(map[int64]*rawBucket),
		cfg:       c,
	}
}

var bucketSize = (10 * time.Second).Nanoseconds()

// alignTs returns the provided timestamp truncated to the bucket size.
// It gives us the start time of the time bucket in which such timestamp falls.
func alignTs(ts int64) int64 { return ts - ts%bucketSize }

func (c *concentrator) Start() {
	defer close(c.stop)
	tick := time.NewTicker(time.Duration(bucketSize) * time.Nanosecond)
	defer tick.Stop()
	go func() {
		for {
			select {
			case now := <-tick.C:
				p := c.flush(now)
				if len(p.Stats) == 0 {
					// nothing to flush
					continue
				}
				addr := fmt.Sprintf("http://%s/v0.5/stats", c.cfg.agentAddr)
				var buf bytes.Buffer
				if err := msgp.Encode(&buf, &p); err != nil {
					log.Error("Error encoding stats payload: %v", err)
					continue
				}
				req, err := http.NewRequest("POST", addr, &buf)
				if err != nil {
					log.Error("Error flushing stats: %v", err)
					continue
				}
				// TODO: use user defined client
				resp, err := defaultClient.Do(req)
				if err != nil {
					log.Error("Error flushing stats: %v", err)
					continue
				}
				fmt.Printf("%#v", resp)
			case <-c.stop:
				return
			}
		}
	}()
	for {
		select {
		case ss := <-c.In:
			btime := alignTs(ss.Start + ss.Duration)
			if btime < c.oldestTs {
				btime = c.oldestTs
			}
			b, ok := c.buckets[btime]
			if !ok {
				b = newRawBucket(uint64(btime))
				c.buckets[btime] = b
			}
			b.handleSpan(ss)
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

func (c *concentrator) flush(timenow time.Time) statsPayload {
	now := timenow.UnixNano()
	sp := statsPayload{
		Hostname: c.cfg.hostname,
		Env:      c.cfg.env,
		Version:  c.cfg.version,
		Stats:    make([]statsBucket, 0, len(c.buckets)),
	}
	c.mu.Lock()
	for ts, srb := range c.buckets {
		// Always keep `bufferLen` buckets (default is 2: current + previous one).
		// This is a trade-off: we accept slightly late traces (clock skew and stuff)
		// but we delay flushing by at most `bufferLen` buckets.
		if ts > now-int64(c.bufferLen)*bucketSize {
			continue
		}
		log.Debug("Flushing bucket %d", ts)
		sp.Stats = append(sp.Stats, srb.Export())
		delete(c.buckets, ts)
	}
	// After flushing, update the oldest timestamp allowed to prevent having stats for
	// an already-flushed bucket.
	newOldestTs := alignTs(now) - int64(c.bufferLen-1)*bucketSize
	if newOldestTs > c.oldestTs {
		log.Debug("Update oldestTs to %d", newOldestTs)
		c.oldestTs = newOldestTs
	}
	c.mu.Unlock()
	return sp
}

type rawBucket struct {
	start, duration uint64
	data            map[aggregation]*rawGroupedStats
}

type rawGroupedStats struct {
	// using float64 here to avoid the accumulation of rounding issues.
	hits            float64
	topLevelHits    float64
	errors          float64
	duration        float64
	okDistribution  *ddsketch.DDSketch
	errDistribution *ddsketch.DDSketch
}

func (s *rawGroupedStats) export(k aggregation) (groupedStats, error) {
	msg := s.okDistribution.ToProto()
	okSummary, err := proto.Marshal(msg)
	if err != nil {
		return groupedStats{}, err
	}
	msg = s.errDistribution.ToProto()
	errSummary, err := proto.Marshal(msg)
	if err != nil {
		return groupedStats{}, err
	}
	// round a float to an int, uniformly choosing
	// between the lower and upper approximations.
	round := func(f float64) uint64 {
		i := uint64(f)
		if rand.Float64() < f-float64(i) {
			i++
		}
		return i
	}
	return groupedStats{
		Service:        k.Service,
		Name:           k.Name,
		Resource:       k.Resource,
		HTTPStatusCode: k.StatusCode,
		Type:           k.Type,
		Hits:           round(s.hits),
		Errors:         round(s.errors),
		Duration:       round(s.duration),
		TopLevelHits:   round(s.topLevelHits),
		OkSummary:      okSummary,
		ErrorSummary:   errSummary,
		Synthetics:     k.Synthetics,
	}, nil
}

func newRawBucket(btime uint64) *rawBucket {
	return &rawBucket{
		start:    btime,
		duration: uint64(bucketSize),
		data:     make(map[aggregation]*rawGroupedStats),
	}
}

func (sb *rawBucket) handleSpan(ss *spanSummary) {
	key := aggregation{
		Name:       ss.Name,
		Env:        ss.Env,
		Type:       ss.Type,
		Resource:   ss.Resource,
		Service:    ss.Service,
		Hostname:   ss.Hostname,
		StatusCode: ss.StatusCode,
		Version:    ss.Version,
		Synthetics: ss.Synthetics,
	}
	gs, ok := sb.data[key]
	if !ok {
		gs = newRawGroupedStats()
		sb.data[key] = gs
	}
	if ss.TopLevel {
		gs.topLevelHits += 1
	}
	gs.hits += 1
	if ss.Error != 0 {
		gs.errors += 1
	}
	gs.duration += float64(ss.Duration)
	// alter resolution of duration distro
	trundur := nsTimestampToFloat(ss.Duration)
	if ss.Error != 0 {
		gs.errDistribution.Add(trundur)
	} else {
		gs.okDistribution.Add(trundur)
	}
}

// Export transforms a RawBucket into a statsBucket, typically used
// before communicating data to the API, as RawBucket is the internal
// type while statsBucket is the public, shared one.
func (sb *rawBucket) Export() statsBucket {
	csb := statsBucket{
		Start:    sb.start,
		Duration: sb.duration,
		Stats:    make([]groupedStats, len(sb.data)),
	}
	for k, v := range sb.data {
		b, err := v.export(k)
		if err != nil {
			log.Error("Could not export stats bucket: %v.", err)
			continue
		}
		csb.Stats = append(csb.Stats, b)
	}
	return csb
}

// nsTimestampToFloat converts a nanosec timestamp into a float nanosecond timestamp truncated to a fixed precision
func nsTimestampToFloat(ns int64) float64 {
	// 10 bits precision (any value will be +/- 1/1024)
	const roundMask int64 = 1 << 10
	var shift uint
	for ns > roundMask {
		ns = ns >> 1
		shift++
	}
	return float64(ns << shift)
}

const (
	// relativeAccuracy is the value accuracy we have on the percentiles. For example, we can
	// say that p99 is 100ms +- 1ms
	relativeAccuracy = 0.01
	// maxNumBins is the maximum number of bins of the ddSketch we use to store percentiles.
	// It can affect relative accuracy, but in practice, 2048 bins is enough to have 1% relative accuracy from
	// 80 micro second to 1 year: http://www.vldb.org/pvldb/vol12/p2195-masson.pdf
	maxNumBins = 2048
)

func newRawGroupedStats() *rawGroupedStats {
	okSketch, err := ddsketch.LogCollapsingLowestDenseDDSketch(relativeAccuracy, maxNumBins)
	if err != nil {
		log.Error("Error when creating ddsketch: %v", err)
	}
	errSketch, err := ddsketch.LogCollapsingLowestDenseDDSketch(relativeAccuracy, maxNumBins)
	if err != nil {
		log.Error("Error when creating ddsketch: %v", err)
	}
	return &rawGroupedStats{
		okDistribution:  okSketch,
		errDistribution: errSketch,
	}
}

type aggregation struct {
	Name       string
	Env        string
	Type       string
	Resource   string
	Service    string
	Hostname   string
	StatusCode uint32
	Version    string
	Synthetics bool
}
