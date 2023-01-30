// Copyright 2021 - 2022 Matrix Origin
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

/*
Package observability init MO's metric, trace, log Collection.
*/
package observability

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

func InitService(ctx context.Context, SV *config.ObservabilityParameters, stopper *stopper.Stopper, fs fileservice.FileService, nodeRole string, nodeUUID string) error {
	var writerFactory table.WriterFactory
	var err error
	var initWG sync.WaitGroup

	if !SV.DisableTrace || !SV.DisableMetric {
		writerFactory = export.GetWriterFactory(fs, nodeUUID, nodeRole, SV.LogsExtension)
		_ = table.SetPathBuilder(ctx, SV.PathBuilder)
	}
	if !SV.DisableTrace {
		initWG.Add(1)
		stopper.RunNamedTask("trace", func(ctx context.Context) {
			if ctx, err = motrace.Init(ctx,
				motrace.WithMOVersion(SV.MoVersion),
				motrace.WithNode(nodeUUID, nodeRole),
				motrace.EnableTracer(!SV.DisableTrace),
				motrace.WithBatchProcessMode(SV.BatchProcessor),
				motrace.WithBatchProcessor(export.NewMOCollector(ctx)),
				motrace.WithFSWriterFactory(writerFactory),
				motrace.WithExportInterval(SV.TraceExportInterval),
				motrace.WithLongQueryTime(SV.LongQueryTime),
				motrace.WithSQLExecutor(nil),
				motrace.DebugMode(SV.EnableTraceDebug),
			); err != nil {
				panic(err)
			}
			initWG.Done()
			<-ctx.Done()
			// flush trace/log/error framework
			if err = motrace.Shutdown(motrace.DefaultContext()); err != nil {
				logutil.Warn("Shutdown trace", logutil.ErrorField(err), logutil.NoReportFiled())
			}
		})
		initWG.Wait()
	}
	if !SV.DisableMetric {
		metric.InitMetric(ctx, nil, SV, nodeUUID, nodeRole, metric.WithWriterFactory(writerFactory),
			metric.WithExportInterval(SV.MetricExportInterval),
			metric.WithUpdateInterval(SV.MetricUpdateStorageUsageInterval.Duration),
			metric.WithMultiTable(SV.MetricMultiTable))
	}
	if err = export.InitMerge(ctx, SV.MergeCycle.Duration, SV.MergeMaxFileSize, SV.MergedExtension); err != nil {
		return err
	}
	return nil
}
