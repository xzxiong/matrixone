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

// ================================================================================
// Make it as simple as possible
// ================================================================================

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/export/v2"
	"sync"
	"time"
)

const MiB = 1 << 20

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

	// content buffer
	key2Buffer map[string]*contentBuffer
	bufferMux  sync.RWMutex
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
		// part buffer
		key2Buffer: make(map[string]*contentBuffer),
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
		case item := <-c.awakeCollect:
			// consume all content
			c.put(item)

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

func (c *CsvExportCollector) put(item v2.Item) {
	buf := c.getBuffer(item.GetName())
	buf.Push(item)
}

var _ v2.Content = (*contentBuffer)(nil)

type contentBuffer struct {
	mux         sync.Mutex
	buffer      *bytes.Buffer
	MaxSize     int
	ReserveSize int
}

type contentBufferOption func(*contentBuffer)

func (o contentBufferOption) Apply(buffer *contentBuffer) {
	o(buffer)
}

func NewContentBuffer(opts ...contentBufferOption) *contentBuffer {
	b := &contentBuffer{
		MaxSize:     10 * MiB,
		ReserveSize: MiB,
	}
	for _, opt := range opts {
		opt.Apply(b)
	}

	// do init
	b.buffer = bytes.NewBuffer(make([]byte, 0, b.MaxSize+b.ReserveSize))
	// End init
	return b
}

func (c *contentBuffer) Push(item v2.Item) {
	//TODO implement me
	panic("implement me")

	// c.Lock()
	// c.WriteBuffer
	// if c.Full() send write signal, refresh buffer
	// -	NewWriteRequest(c.buffer)
	// -	c.buffer = bytes.NewBuffer(make([]byte, 0, c.MaxSize+c.ReserveSize))
	// c.Unlock()
}

func (c *contentBuffer) Full() bool {
	//TODO implement me
	panic("implement me")
}

func (c *contentBuffer) isFull() bool {
	return c.buffer.Len() > c.MaxSize
}

func (c *CsvExportCollector) getBuffer(key string) *contentBuffer {
	c.bufferMux.RLock()
	buf, exist := c.key2Buffer[key]
	if !exist {
		c.bufferMux.RUnlock()
		c.bufferMux.Lock()
		if buf, exist = c.key2Buffer[key]; !exist {
			// TODO: register prepare function
			// case: statement_info need aggr
			buf = NewContentBuffer()
			c.key2Buffer[key] = buf
		}
		c.bufferMux.Unlock()
		c.bufferMux.RLock()
	}
	c.bufferMux.RUnlock()
	return buf
}

type exportRequest struct {
}

func NewExportRequest() *exportRequest {
	return &exportRequest{}
}

func (e *exportRequest) GetName() string {
	return "export"
}

func (e *exportRequest) GetContent() v2.Content {
	return nil
}

func (e *exportRequest) GetContentType() string {
	return "text/csv"
}

func (e *exportRequest) GetContentEncoding() string {
	return "utf-8"
}
