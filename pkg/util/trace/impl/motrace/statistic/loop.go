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

import "time"

const (
	ADD    string = "add"
	REMOVE string = "remove"
)

type Generator struct {
	c chan *event

	content map[string]map[string]*event

	delayInterval time.Duration
	window        time.Duration
}

type event struct {
	typ     string
	tempId  string
	queryId string
	stats   *StatsInfo
	last    time.Time
}

func NewGenerator() *Generator {
	return &Generator{
		c: make(chan *event, 100),
	}
}

func (g *Generator) Register(templateId string, queryId string, info *StatsInfo) {
	g.c <- &event{
		typ:     ADD,
		tempId:  templateId,
		queryId: queryId,
		stats:   info,
		last:    time.Now(),
	}
}

func (g *Generator) Remove(templateId, queryId string) {
	g.c <- &event{
		typ:     REMOVE,
		tempId:  templateId,
		queryId: queryId,
	}

}

func (g *Generator) Loop() {

	// 1. each window generate this window record
	// 2. need delay_interval, for  the end check
	// 3. need to exclude finished query.

	var ticker = time.NewTicker(g.delayInterval)

	for {
		select {
		case e := <-g.c:
			switch e.typ {
			case ADD:
				_, exist := g.content[e.tempId]
				if !exist {
					g.content[e.tempId] = make(map[string]*event, 12)
				}
				g.content[e.tempId][e.queryId] = e
				e.queryId = ""
				e.tempId = ""
			case REMOVE:
				_, exist := g.content[e.tempId]
				if exist {
					delete(g.content[e.tempId], e.queryId)
				}
			}

		case <-ticker.C:
			// record last window.
			windowEnd := time.Now().Add(-g.delayInterval).Truncate(g.window)
			for _, cc := range g.content {
				for _, e := range cc {
					if e.last.Before(windowEnd) {
						reportStatementCpu(e.stats, CpuType, e.last, windowEnd)
						e.last = windowEnd
					}
				}
			}

		}
	}

}
