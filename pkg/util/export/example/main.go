// Copyright 2022 Matrix Origin
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

// Bin to show how Merge Task work. Helps to debug code, or optimization.
//
// There are two type table data: one is dummyStatementTable, with primary key; another is dummyRawlogTable, without primary key.
// Both of them are code-copied from pkg "github.com/matrixorigin/matrixone/pkg/util/trace", caused by import-cycle issue.
//
// If you want to run this code, you need some source data, that could copy from folder `mo-data/etl/sys/logs/...`, which generated by mo-service startup with config `etc/launch-tae-logservice`
package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"go.uber.org/zap/zapcore"

	morun "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

func main() {

	ctx := context.Background()
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:      zapcore.InfoLevel.String(),
		Format:     "console",
		Filename:   "",
		MaxSize:    512,
		MaxDays:    0,
		MaxBackups: 0,

		DisableStore: true,
	})

	fs, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, "mo-data/etl")
	if err != nil {
		logutil.Infof("failed open fileservice: %v", err)
		return
	}
	files, err := fs.List(ctx, "/")
	if err != nil {
		logutil.Infof("failed list /: %v", err)
		return
	}
	if len(files) == 0 {
		logutil.Infof("skipping, no mo-data/etl folder")
		return
	}

	httpWG := sync.WaitGroup{}
	httpWG.Add(1)
	go func() {
		httpWG.Done()
		http.ListenAndServe("0.0.0.0:8123", nil)
	}()
	httpWG.Wait()
	time.Sleep(time.Second)

	ctx, cancel := context.WithCancel(ctx)
	go traceMemStats(ctx)

	SV := config.NewObservabilityParameters()
	SV.SetDefaultValues("test")
	SV.MergeCycle.Duration = 5 * time.Minute
	if err := export.InitMerge(ctx, SV); err != nil {
		panic(err)
	}
	dr := morun.NewRuntime(
		metadata.ServiceType_CN,
		"",
		logutil.GetGlobalLogger(),
		morun.WithClock(clock.NewHLCClock(func() int64 {
			return time.Now().UTC().UnixNano()
		}, 0)))
	morun.SetupProcessLevelRuntime(dr)
	err = motrace.Init(ctx, motrace.EnableTracer(true))
	if err != nil {
		panic(err)
	}

	mergeAll(ctx, fs)

	mergeTable(ctx, fs, motrace.SingleStatementTable)
	mergeTable(ctx, fs, motrace.SingleRowLogTable)
	mergeTable(ctx, fs, mometric.SingleMetricTable)

	logutil.Infof("all done, run sleep(5)")
	time.Sleep(5 * time.Second)
	cancel()
}

func mergeAll(ctx context.Context, fs *fileservice.LocalETLFS) {
	ctx, span := trace.Start(ctx, "mergeTable")
	defer span.End()
	var err error
	var merge *export.Merge
	merge, err = export.NewMerge(ctx, export.WithTable(motrace.SingleStatementTable), export.WithFileService(fs))
	if err != nil {
		logutil.Infof("[%v] failed to NewMerge: %v", "All", err)
	}
	err = merge.Main(ctx)
	if err != nil {
		logutil.Infof("[%v] failed to merge: %v", "All", err)
	} else {
		logutil.Infof("[%v] merge succeed.", "All")
	}

	writeAllocsProfile("All")

}

func mergeTable(ctx context.Context, fs *fileservice.LocalETLFS, table *table.Table) {
	var err error
	ctx, span := trace.Start(ctx, "mergeTable")
	defer span.End()
	merge, err := export.NewMerge(ctx, export.WithTable(table), export.WithFileService(fs))
	logutil.Infof("[%v] create merge task, err: %v", table.GetName(), err)
	ts, err := time.Parse("2006-01-02 15:04:05", "2023-01-03 00:00:00")
	logutil.Infof("[%v] create ts: %v, err: %v", table.GetName(), ts, err)
	err = merge.Main(ctx)
	if err != nil {
		logutil.Infof("[%v] failed to merge: %v", table.GetName(), err)
	} else {
		logutil.Infof("[%v] merge succeed.", table.GetName())
	}

	writeAllocsProfile(table.GetName())
}

func writeAllocsProfile(suffix string) {
	profile := pprof.Lookup("heap")
	if profile == nil {
		return
	}
	profilePath := ""
	if profilePath == "" {
		profilePath = "heap-profile"
	}
	if len(suffix) > 0 {
		profilePath = profilePath + "." + suffix
	}
	f, err := os.Create(profilePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := profile.WriteTo(f, 0); err != nil {
		panic(err)
	}
	logutil.Infof("Allocs profile written to %s", profilePath)
}

func traceMemStats(ctx context.Context) {
	var ms runtime.MemStats
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-time.After(time.Second):
			runtime.ReadMemStats(&ms)
			logutil.Infof("Alloc:%10d(bytes) HeapIdle:%10d(bytes) HeapReleased:%10d(bytes)", ms.Alloc, ms.HeapIdle, ms.HeapReleased)
		}
	}
}
