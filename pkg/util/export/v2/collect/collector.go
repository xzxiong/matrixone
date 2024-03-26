// Copyright 2024 Matrix Origin
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

package collect

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/export/v2"
	"time"
)

const defaultQueueSize = 1310720 // queue mem cost = 10MB

const LoggerNameMOCollector = "MOCollector"

const discardCollectTimeout = time.Millisecond

var _ v2.Collector = (*CsvExportCollector)(nil)

// CsvExportCollector collect, serialize, export items.
// Collector decide how to export items.
type CsvExportCollector struct {
	// writerFactory is used to create a writer for each item.
	// given 2 ways to export data: one for normal case, one for failure case.
	writerFactory   table.WriterFactory
	regularInterval time.Duration

	ctx          context.Context
	stopCh       chan struct{}
	awakeCollect chan v2.Item
	awakeRegular *time.Ticker
	awakeFull    chan interface{}
	awakeExport  chan interface{}
}

type CollectorOption func(*CsvExportCollector)

func (opt CollectorOption) Apply(c *CsvExportCollector) { opt(c) }

const defaultRegularInterval = 15 * time.Second

func NewCollector(ctx context.Context, options ...CollectorOption) *CsvExportCollector {
	c := &CsvExportCollector{
		ctx:             ctx,
		regularInterval: defaultRegularInterval,
		stopCh:          make(chan struct{}),
		awakeCollect:    make(chan v2.Item, defaultQueueSize),
		awakeRegular:    time.NewTicker(defaultRegularInterval),
		awakeExport:     make(chan interface{}),
	}
	for _, opt := range options {
		opt.Apply(c)
	}
	return c
}

func (c *CsvExportCollector) Collect(ctx context.Context, item v2.Item) error {
	select {
	case <-c.stopCh:
		ctx = errutil.ContextWithNoReport(ctx, true)
		return moerr.NewInternalError(ctx, "MOCollector stopped")
	case c.awakeCollect <- item:
		return nil
	}
}

func (c *CsvExportCollector) Start(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func (c *CsvExportCollector) Stop(graceful bool) {
	//TODO implement me
	panic("implement me")
}

// loop is the main loop of the collector.
// TODO: 怎么控制worker的个数 ?
func (c *CsvExportCollector) loop() {

	for {
		select {
		case <-c.stopCh:
			// quit
			return
		case <-c.awakeCollect:
			// consume all content
			c.consumeBuffer()

		case <-c.awakeRegular.C:
			// handle regular trigger

			c.awakeRegular.Reset(c.regularInterval)
		case <-c.awakeFull:
			// handle full trigger
			// need timeout branch, if timeout just write files.

		case <-c.awakeExport:
			// handle export
		}
	}
}

func (c *CsvExportCollector) consumeBuffer() {
	nextTime := time.Now().Add(time.Second)
	for {
		for {
			case <- c.awakeCollect:
		}
	}
}
