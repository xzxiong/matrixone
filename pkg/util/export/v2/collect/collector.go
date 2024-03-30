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
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
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

	ctx     context.Context
	stopCtx context.Context
	cancel  context.CancelFunc

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
		awakeCollect:    make(chan v2.Item, defaultQueueSize),
		awakeRegular:    time.NewTicker(defaultRegularInterval),
		awakeExport:     make(chan interface{}),
		// part buffer
		key2Buffer: make(map[string]*contentBuffer),
	}
	for _, opt := range options {
		opt.Apply(c)
	}

	c.stopCtx, c.cancel = context.WithCancel(ctx)
	return c
}

func (c *CsvExportCollector) Collect(ctx context.Context, item v2.Item) error {
	select {
	case <-c.stopCtx.Done():
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
		case <-c.stopCtx.Done():
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
	ctx     context.Context
	stopCtx context.Context
	cancel  context.CancelFunc

	mux         sync.Mutex
	writer      table.RowWriter
	buffer      *bytes.Buffer
	MaxSize     int
	ReserveSize int

	filters        []filterFunc
	getter         []getterFunc
	gatherInterval time.Duration

	// from Collector
	writerFactory table.WriterFactory
	collector     *CsvExportCollector
}

type filterFunc func(v2.Item) bool
type getterFunc func() []v2.Item

type contentBufferOption func(*contentBuffer)

func (o contentBufferOption) Apply(buffer *contentBuffer) {
	o(buffer)
}

func NewContentBuffer(ctx context.Context, opts ...contentBufferOption) *contentBuffer {
	b := &contentBuffer{
		ctx:         ctx,
		MaxSize:     10 * MiB,
		ReserveSize: MiB,
	}
	for _, opt := range opts {
		opt.Apply(b)
	}

	// do init
	b.stopCtx, b.cancel = context.WithCancel(ctx)

	// End init

	go b.loop()

	return b
}

func (b *contentBuffer) Push(item v2.Item) {
	//TODO implement me
	panic("implement me")

	for _, filter := range b.filters {
		// TODO: need other way to collect filtered-item
		if filter(item) {
			return
		}
	}

	b.WriteItem(item)
}

func (b *contentBuffer) WriteItem(item v2.Item) {
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.buffer == nil {
		b.buffer = bytes.NewBuffer(make([]byte, 0, b.MaxSize+b.ReserveSize))
		b.writer = etl.NewCSVWriterWithByteBuffer(b.ctx, b.buffer)
	}

	row := item.GetTable().GetRow(b.ctx)
	item.FillRow(b.ctx, row)
	//account := row.GetAccount()
	//b.writerFactory.GetRowWriter(b.ctx, account, row.Table, ts)

	// b.WriteBuffer
	b.writer.WriteRow(row)
	// if b.Full() send write signal, refresh buffer
	// -	NewWriteRequest(b.buffer)
	// -	b.buffer = bytes.NewBuffer(make([]byte, 0, b.MaxSize+b.ReserveSize))
	if b.Full() {
		b.writer.FlushAndClose()
		req := NewExportRequest(b.buffer)
		b.collector.awakeFull <- req
		b.writer = nil
		b.buffer = nil
	}
}

func (b *contentBuffer) Full() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.isFull()
}

func (b *contentBuffer) isFull() bool {
	return b.buffer.Len() > b.MaxSize
}

func (b *contentBuffer) willFull(size int) bool {
	return b.buffer.Len()+size > b.MaxSize
}

func (b *contentBuffer) loop() {
	if len(b.getter) == 0 {
		return
	}
	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-time.After(b.gatherInterval):
			for _, g := range b.getter {
				items := g()
				for _, item := range items {
					b.WriteItem(item)
					// fixme metric counter
				}
			}
		}
	}
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
			buf = NewContentBuffer(c.ctx)
			c.key2Buffer[key] = buf
		}
		c.bufferMux.Unlock()
		c.bufferMux.RLock()
	}
	c.bufferMux.RUnlock()
	return buf
}

//var _ table.RowWriter = (*exportRequest)(nil)

// exportRequest is bound to contentBuffer.
type exportRequest struct {
	buffer *bytes.Buffer
}

func NewExportRequest(buf *bytes.Buffer) *exportRequest {
	return &exportRequest{buffer: buf}
}

// DoLoad exec `Load data infile...` to write into MO.
func (r *exportRequest) DoLoad() error {
	panic("Implement me")
}

// DoExportFile export file to remote storage, like: S3.
func (r *exportRequest) DoExportFile() error {
	panic("Implement me")
}
