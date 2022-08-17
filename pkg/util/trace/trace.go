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

package trace

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

var gTracerProvider *MOTracerProvider
var gTracer Tracer
var gTraceContext = context.Background()
var gSpanContext atomic.Value

func Init(ctx context.Context, opts ...TracerProviderOption) (context.Context, error) {

	// init tool dependence
	logutil.SetLogReporter(&logutil.TraceReporter{ReportLog: ReportLog, ReportZap: ReportZap, LevelSignal: SetLogLevel, ContextField: ContextField})
	logutil.SpanFieldKey.Store(SpanFieldKey)
	errors.SetErrorReporter(HandleError)
	export.SetDefaultContextFunc(DefaultContext)

	// init TraceProvider
	gTracerProvider = newMOTracerProvider(opts...)
	config := &gTracerProvider.tracerProviderConfig

	// init Tracer
	gTracer = gTracerProvider.Tracer("MatrixOrigin")

	// init Node DefaultContext
	var spanId SpanID
	spanId.SetByUUID(config.getNodeResource().NodeUuid)
	sc := SpanContextWithIDs(nilTraceID, spanId)
	gSpanContext.Store(&sc)
	gTraceContext = ContextWithSpanContext(ctx, sc)

	if err := initExport(ctx, config); err != nil {
		return nil, err
	}

	return gTraceContext, nil
}

func initExport(ctx context.Context, config *tracerProviderConfig) error {
	if !config.IsEnable() {
		logutil.Info("initExport pass.")
		return nil
	}
	var p export.BatchProcessor
	// init BatchProcess for trace/log/error
	switch {
	case config.batchProcessMode == InternalExecutor:
		// init schema
		if err := InitSchemaByInnerExecutor(ctx, config.sqlExecutor); err != nil {
			return err
		}
		// register buffer pipe implements
		export.Register(&MOSpan{}, NewBufferPipe2SqlWorker(
			bufferWithSizeThreshold(MB),
		))
		export.Register(&MOLog{}, NewBufferPipe2SqlWorker())
		export.Register(&MOZap{}, NewBufferPipe2SqlWorker())
		export.Register(&StatementInfo{}, NewBufferPipe2SqlWorker())
		export.Register(&MOErrorHolder{}, NewBufferPipe2SqlWorker())
	case config.batchProcessMode == FileService:
		if GetNodeResource().NodeType == NodeTypeNode || GetNodeResource().NodeType == NodeTypeCN {
			// only in standalone mode or CN node can init schema
			if err := InitExternalTblSchema(ctx, config.sqlExecutor, config.fsConfig); err != nil {
				return err
			}
		}
		export.Register(&MOSpan{}, NewBufferPipe2CSVWorker())
		export.Register(&MOLog{}, NewBufferPipe2CSVWorker())
		export.Register(&MOZap{}, NewBufferPipe2CSVWorker())
		export.Register(&StatementInfo{}, NewBufferPipe2CSVWorker())
		export.Register(&MOErrorHolder{}, NewBufferPipe2CSVWorker())
	default:
		return moerr.NewPanicError(fmt.Errorf("unknown batchProcessMode: %s", config.batchProcessMode))
	}
	logutil.Info("init GlobalBatchProcessor")
	// init BatchProcessor for standalone mode.
	p = export.NewMOCollector()
	export.SetGlobalBatchProcessor(p)
	if !p.Start() {
		return moerr.NewPanicError("trace exporter already started")
	}
	config.spanProcessors = append(config.spanProcessors, NewBatchSpanProcessor(p))
	logutil.Info("init trace span processor")
	return nil
}

func Shutdown(ctx context.Context) error {
	if !gTracerProvider.IsEnable() {
		return nil
	}

	gTracerProvider.EnableTracer(false)
	tracer := noopTracer{}
	_ = atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(gTracer.(*MOTracer))), unsafe.Pointer(&tracer))

	// fixme: need stop timeout
	return export.GetGlobalBatchProcessor().Stop(true)
}

func Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return gTracer.Start(ctx, spanName, opts...)
}

func DefaultContext() context.Context {
	return gTraceContext
}

func DefaultSpanContext() *SpanContext {
	return gSpanContext.Load().(*SpanContext)
}

func GetNodeResource() *MONodeResource {
	return gTracerProvider.getNodeResource()
}
