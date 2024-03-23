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
	writerFactory table.WriterFactory
}

type CollectorOption func(*CsvExportCollector)

func (opt CollectorOption) Apply(c *CsvExportCollector) { opt(c) }

func NewCollector(options ...CollectorOption) *CsvExportCollector {
	c := &CsvExportCollector{}
	for _, opt := range options {
		opt.Apply(c)
	}
	return c
}

func (c *CsvExportCollector) Collect(ctx context.Context, item v2.Item) {
	//TODO implement me
	panic("implement me")
}

func (c *CsvExportCollector) Start(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func (c *CsvExportCollector) Stop(graceful bool) {
	//TODO implement me
	panic("implement me")
}
