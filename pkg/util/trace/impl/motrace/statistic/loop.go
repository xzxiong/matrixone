// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistic

import (
	"context"
	"sync"
	"time"
)

const (
	ADD    string = "add"
	REMOVE string = "remove"
)

type Generator struct {
	stopped bool
	c       chan *event

	content map[uint64]*event

	delayInterval time.Duration
	window        time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	once          sync.Once
}

type event struct {
	typ    string
	connId uint64
	stats  *StatsInfo
	last   time.Time
}

func NewGenerator() *Generator {
	return &Generator{
		c:       make(chan *event, 100),
		stopped: false,
	}
}

func (g *Generator) Start(ctx context.Context) {
	g.once.Do(func() {
		if g.stopped {
			return
		}
		g.ctx, g.cancel = context.WithCancel(ctx)
		go g.loop(ctx)
	})
}

func (g *Generator) Stop() {
	if !g.stopped {
		g.cancel()
		g.stopped = true
	}
}

func (g *Generator) Register(connId uint64, info *StatsInfo) {
	g.c <- &event{
		typ:    ADD,
		connId: connId,
		stats:  info,
		last:   time.Now(),
	}
}

func (g *Generator) Remove(connId uint64) {
	g.c <- &event{
		typ:    REMOVE,
		connId: connId,
	}

}

func (g *Generator) loop(ctx context.Context) {

	// 1. each window generate this window record
	// 2. need delay_interval, for  the end check
	// 3. need to exclude finished query.

	var ticker = time.NewTicker(g.delayInterval)

	for {
		select {
		case e := <-g.c:
			switch e.typ {
			case ADD:
				g.content[e.connId] = e
			case REMOVE:
				delete(g.content, e.connId)
			}

		case <-ticker.C:
			// record last window.
			windowEnd := time.Now().Add(-g.delayInterval).Truncate(g.window)
			for _, e := range g.content {
				if e.last.Before(windowEnd) {
					reportStatementCpu(e.stats, CpuType, e.last, windowEnd)
					e.last = windowEnd
				}
			}
		}
	}

}

var generator Generator

func Register(connId uint32, info *StatsInfo) {
	generator.Register(uint64(connId), info)
}

func Remove(connId uint32) {
	generator.Remove(uint64(connId))
}
