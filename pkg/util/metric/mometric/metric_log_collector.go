// Copyright 2023 Matrix Origin
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

package mometric

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"go.uber.org/zap"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var _ MetricCollector = (*metricLogCollector)(nil)

type metricLogCollector struct {
	*bp.BaseBatchPipe[*pb.MetricFamily, table.ExportRequests]
	opts collectorOpts
}

func (c *metricLogCollector) SendMetrics(ctx context.Context, mfs []*pb.MetricFamily) error {
	for _, mf := range mfs {
		if err := c.SendItem(ctx, mf); err != nil {
			return err
		}
	}
	return nil
}

func newMetricLogCollector(opts ...collectorOpt) MetricCollector {
	initOpts := defaultCollectorOpts()
	for _, o := range opts {
		o.ApplyTo(&initOpts)
	}
	c := &metricLogCollector{
		opts: initOpts,
	}
	pipeOpts := []bp.BaseBatchPipeOpt{bp.PipeWithBatchWorkerNum(c.opts.sqlWorkerNum)}
	if !initOpts.multiTable {
		pipeOpts = append(pipeOpts,
			bp.PipeWithBufferWorkerNum(1),
			bp.PipeWithItemNameFormatter(func(bp.HasName) string {
				return SingleMetricTable.GetName()
			}))
	}
	base := bp.NewBaseBatchPipe[*pb.MetricFamily, table.ExportRequests](c, pipeOpts...)
	c.BaseBatchPipe = base
	return c
}

func (c *metricLogCollector) NewItemBatchHandler(ctx context.Context) func(batch table.ExportRequests) {
	return func(batchs table.ExportRequests) {}
}

func (c *metricLogCollector) NewItemBuffer(_ string) bp.ItemBuffer[*pb.MetricFamily, table.ExportRequests] {
	return &mfsetLog{
		mfset: mfset{
			Reminder:        bp.NewConstantClock(c.opts.flushInterval),
			metricThreshold: c.opts.metricThreshold,
			sampleThreshold: c.opts.sampleThreshold,
		},
		collector: c,
		logger:    runtime.ProcessLevelRuntime().Logger().Named("MetricLog"),
	}
}

type mfsetLog struct {
	mfset
	collector *metricLogCollector
	logger    *log.MOLogger
}

func (s *mfsetLog) GetBatch(ctx context.Context, buf *bytes.Buffer) table.ExportRequests {
	buf.Reset()

	elems := []map[string]any{}

	for _, mf := range s.mfs {
		for _, metric := range mf.Metric {

			elem := map[string]any{}
			elem["name"] = mf.GetName()
			for _, lbl := range metric.Label {
				elem[lbl.GetName()] = lbl.GetValue()
			}
			elem["time"] = localTime(metric.GetCollecttime())

			switch mf.GetType() {
			case pb.MetricType_COUNTER:
				elem["value"] = metric.Counter.GetValue()
				elem["time"] = localTime(metric.GetCollecttime())
			case pb.MetricType_GAUGE:
				elem["value"] = metric.Gauge.GetValue()
				elem["time"] = localTime(metric.GetCollecttime())
			case pb.MetricType_RAWHIST:
				for _, sample := range metric.RawHist.Samples {

					targetMap := make(map[string]any)
					// Copy from the original map to the target map
					for key, value := range elem {
						targetMap[key] = value
					}
					targetMap["time"] = localTime(sample.GetDatetime())
					targetMap["value"] = sample.GetValue()
					elems = append(elems, targetMap)
				}
			default:
				panic(moerr.NewInternalError(ctx, "unsupported metric type %v", mf.GetType()))
			}
			elems = append(elems, elem)
		}
	}

	// log like zap field (elems need to implement zapcore.ObjectMarshaler)
	s.logger.Log("metric_log", zap.Object("stat", elems))

	return nil
}

func localTime(value int64) time.Time {
	return time.UnixMicro(value).In(time.Local)
}

func localTimeStr(value int64) string {
	return time.UnixMicro(value).In(time.Local).Format("2006-01-02 15:04:05.000000")
}
