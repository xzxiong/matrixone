// Copyright 2021 - 2023 Matrix Origin
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
	"strconv"
	"sync/atomic"
	"time"
)

type StatsArray [StatsArrayLength]float64

const (
	Decimal128ToFloat64Scale = 5
	Float64PrecForMemorySize = 3
	Float64PrecForCU         = 4
	Float64PrecForIOInput    = 6
)

const StatsArrayVersion = StatsArrayVersionLatest

const (
	StatsArrayVersion0 = iota // raw statistics

	StatsArrayVersion1 = 1 // float64 array
	StatsArrayVersion2 = 2 // float64 array + plus one elem OutTrafficBytes
	StatsArrayVersion3 = 3 // ... + 1 elem: ConnType
	StatsArrayVersion4 = 4 // ... + 2 elem: OutPacketCount, CU
	StatsArrayVersion5 = 5 // ... + 1 elem: S3IOListCount, S3IODeleteCount

	StatsArrayVersionLatest // same value as last variable StatsArrayVersion#
)

const (
	StatsArrayIndexVersion = iota
	StatsArrayIndexTimeConsumed
	StatsArrayIndexMemorySize
	StatsArrayIndexS3IOInputCount
	StatsArrayIndexS3IOOutputCount // index: 4
	StatsArrayIndexOutTrafficBytes // index: 5
	StatsArrayIndexConnType        // index: 6
	StatsArrayIndexOutPacketCnt    // index: 7, version: 4
	StatsArrayIndexCU              // index: 8, version: 4
	StatsArrayIndexS3IOListCount   // index: 9, version: 5
	StatsArrayIndexS3IODeleteCount // index: 10, version: 5

	StatsArrayLength
)

const (
	StatsArrayLengthV1 = 5
	StatsArrayLengthV2 = 6
	StatsArrayLengthV3 = 7
	StatsArrayLengthV4 = 9
	StatsArrayLengthV5 = 11
)

type ConnType float64

const (
	ConnTypeUnknown  ConnType = 0
	ConnTypeInternal ConnType = 1
	ConnTypeExternal ConnType = 2
)

func NewStatsArray() *StatsArray {
	var s StatsArray
	return s.Init()
}

func NewStatsArrayV1() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion1)
}

func NewStatsArrayV2() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion2)
}

func NewStatsArrayV3() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion3)
}

func NewStatsArrayV4() *StatsArray {
	return NewStatsArray().WithVersion(StatsArrayVersion4)
}

func (s *StatsArray) Init() *StatsArray {
	return s.WithVersion(StatsArrayVersion)
}

func (s *StatsArray) InitIfEmpty() *StatsArray {
	for i := 1; i < StatsArrayLength; i++ {
		if s[i] != 0 {
			return s
		}
	}
	return s.WithVersion(StatsArrayVersion)
}

func (s *StatsArray) Reset() *StatsArray {
	*s = *initStatsArray
	return s
}

func (s *StatsArray) GetVersion() float64         { return (*s)[StatsArrayIndexVersion] }
func (s *StatsArray) GetTimeConsumed() float64    { return (*s)[StatsArrayIndexTimeConsumed] }    // unit: ns
func (s *StatsArray) GetMemorySize() float64      { return (*s)[StatsArrayIndexMemorySize] }      // unit: byte
func (s *StatsArray) GetS3IOInputCount() float64  { return (*s)[StatsArrayIndexS3IOInputCount] }  // unit: count
func (s *StatsArray) GetS3IOOutputCount() float64 { return (*s)[StatsArrayIndexS3IOOutputCount] } // unit: count
func (s *StatsArray) GetOutTrafficBytes() float64 { // unit: byte
	if s.GetVersion() < StatsArrayVersion2 {
		return 0
	}
	return (*s)[StatsArrayIndexOutTrafficBytes]
}
func (s *StatsArray) GetConnType() float64 {
	if s.GetVersion() < StatsArrayVersion3 {
		return 0
	}
	return (*s)[StatsArrayIndexConnType]
}
func (s *StatsArray) GetOutPacketCount() float64 {
	if s.GetVersion() < StatsArrayVersion4 {
		return 0
	}
	return s[StatsArrayIndexOutPacketCnt]
}
func (s *StatsArray) GetCU() float64 {
	if s.GetVersion() < StatsArrayVersion4 {
		return 0
	}
	return s[StatsArrayIndexCU]
}
func (s *StatsArray) GetS3IOListCount() float64 {
	if s.GetVersion() < StatsArrayVersion5 {
		return 0
	}
	return s[StatsArrayIndexS3IOListCount]
}
func (s *StatsArray) GetS3IODeleteCount() float64 {
	if s.GetVersion() < StatsArrayVersion5 {
		return 0
	}
	return s[StatsArrayIndexS3IODeleteCount]
}

// WithVersion set the version array in StatsArray, please carefully to use.
func (s *StatsArray) WithVersion(v float64) *StatsArray { (*s)[StatsArrayIndexVersion] = v; return s }
func (s *StatsArray) WithTimeConsumed(v float64) *StatsArray {
	(*s)[StatsArrayIndexTimeConsumed] = v
	return s
}
func (s *StatsArray) WithMemorySize(v float64) *StatsArray {
	(*s)[StatsArrayIndexMemorySize] = v
	return s
}
func (s *StatsArray) WithS3IOInputCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOInputCount] = v
	return s
}
func (s *StatsArray) WithS3IOOutputCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOOutputCount] = v
	return s
}
func (s *StatsArray) WithS3IOListCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IOListCount] = v
	return s
}
func (s *StatsArray) WithS3IODeleteCount(v float64) *StatsArray {
	(*s)[StatsArrayIndexS3IODeleteCount] = v
	return s
}

func (s *StatsArray) WithOutTrafficBytes(v float64) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion2 {
		(*s)[StatsArrayIndexOutTrafficBytes] = v
	}
	return s
}

func (s *StatsArray) WithConnType(v ConnType) *StatsArray {
	if s.GetVersion() >= StatsArrayVersion3 {
		(*s)[StatsArrayIndexConnType] = float64(v)
	}
	return s
}

func (s *StatsArray) WithOutPacketCount(v float64) *StatsArray {
	s[StatsArrayIndexOutPacketCnt] = v
	return s
}

func (s *StatsArray) WithCU(v float64) *StatsArray {
	s[StatsArrayIndexCU] = v
	return s
}

func (s *StatsArray) ToJsonString() []byte {
	switch s.GetVersion() {
	case StatsArrayVersion1:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV1])
	case StatsArrayVersion2:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV2])
	case StatsArrayVersion3:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV3])
	case StatsArrayVersion4:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV4])
	case StatsArrayVersion5:
		return StatsArrayToJsonString((*s)[:StatsArrayLengthV5])
	default:
		return StatsArrayToJsonString((*s)[:])
	}
}

// Add do add two stats array together
// except for Element ConnType, which idx = StatsArrayIndexConnType, just keep s[StatsArrayIndexConnType] value.
func (s *StatsArray) Add(delta *StatsArray) *StatsArray {
	dstLen := len(*delta)
	if len(*s) < len(*delta) {
		dstLen = len(*s)
	}
	for idx := 1; idx < dstLen; idx++ {
		if idx == StatsArrayIndexConnType {
			continue
		}
		(*s)[idx] += (*delta)[idx]
	}
	return s
}

// StatsArrayToJsonString return json arr format
// example:
// [1,0,0,0,0] got `[1,0,0,0,0]`
// [1,2,3,4,5] got `[1,2,3.000,4,5]`
// [2,1,2,3,4,5] got `[2,3.000,4,5,6.000,7]`
func StatsArrayToJsonString(arr []float64) []byte {
	// len([1,184467440737095516161,18446744073709551616,18446744073709551616,18446744073709551616]") = 88
	buf := make([]byte, 0, 128)
	buf = append(buf, '[')
	for idx, v := range arr {
		if idx > 0 {
			buf = append(buf, ',')
		}
		if v == 0.0 {
			buf = append(buf, '0')
		} else if idx == StatsArrayIndexMemorySize {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForMemorySize, 64)
		} else if idx == StatsArrayIndexCU {
			buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForCU, 64)
		} else if idx == StatsArrayIndexS3IOInputCount {
			if float64(int(v)) == v {
				buf = strconv.AppendFloat(buf, v, 'f', 0, 64)
			} else {
				buf = strconv.AppendFloat(buf, v, 'f', Float64PrecForIOInput, 64)
			}
		} else {
			buf = strconv.AppendFloat(buf, v, 'f', 0, 64)
		}
	}
	buf = append(buf, ']')
	return buf
}

var initStatsArray = NewStatsArray()

var DefaultStatsArray = *initStatsArray

var DefaultStatsArrayJsonString = initStatsArray.ToJsonString()

type statsInfoKey struct{}

type StatsMetadata interface {
	GetAccount() string
	GetStatementFingerprint() string
	GetStatementTemplateId() string
	Lock()
	Unlock()
}

type StatsInfo struct {
	EnableCollect bool `json:"-"`

	Metadata StatsMetadata `json:"-"`

	ParseStage struct {
		ParseDuration  time.Duration `json:"ParseDuration"`
		ParseStartTime time.Time     `json:"ParseStartTime"`
	}

	// Planning Phase Statistics
	PlanStage struct {
		PlanDuration                time.Duration `json:"PlanDuration"`
		PlanStartTime               time.Time     `json:"PlanStartTime"`
		BuildPlanS3Request          S3Request     `json:"BuildPlanS3Request"`
		BuildPlanStatsIOConsumption int64         `json:"BuildPlanStatsIOConsumption"` // unit: ns
		// The following attributes belong to independent statistics during the `buildPlan` stage, only for analysis reference.
		BuildPlanStatsS3              S3Request `json:"BuildPlanStatsS3"`
		BuildPlanStatsDuration        int64     `json:"BuildPlanStatsDuration"`        // unit: ns
		BuildPlanStatsInCacheDuration int64     `json:"BuildPlanStatsInCacheDuration"` // unit: ns
		BuildPlanResolveVarDuration   int64     `json:"BuildPlanResolveVarDuration"`   // unit: ns
	}

	// Compile phase statistics
	CompileStage struct {
		CompileDuration       time.Duration `json:"CompileDuration"`
		CompileStartTime      time.Time     `json:"CompileStartTime"`
		CompileS3Request      S3Request     `json:"CompileS3Request"`
		CompileExpandRangesS3 S3Request     `json:"CompileExpandRangesS3"`
		// It belongs to independent statistics, which occurs during the `CompileQuery` stage, only for analysis reference.
		CompileTableScanDuration int64 `json:"CompileTableScanDuration"` // unit: ns
	}

	// Prepare execution phase statistics
	PrepareRunStage struct {
		CompilePreRunOnceDuration int64 `json:"CompilePreRunOnceDuration"` // unit: ns
		// During Compile PreRun, wait for the lock time when executing `locktable` and `lockMetaTables`
		CompilePreRunOnceWaitLock int64 `json:"CompilePreRunOnceWaitLock"` // unit: ns

		// ScopePrepareDuration belongs to concurrent merge time
		ScopePrepareDuration  int64     `json:"ScopePrepareDuration"` // unit: ns
		ScopePrepareS3Request S3Request `json:"ScopePrepareS3Request"`
		// It belongs to independent statistics, which occurs during the `PrepareRun` stage, only for analysis reference.
		BuildReaderDuration int64 `json:"BuildReaderDuration"` // unit: ns
	}

	// Execution phase statistics
	ExecuteStage struct {
		ExecutionDuration  time.Duration `json:"ExecutionDuration"`
		ExecutionStartTime time.Time     `json:"ExecutionStartTime"`
		ExecutionEndTime   time.Time     `json:"ExecutionEndTime"`

		// time consumption of output operator response to the query result set
		OutputDuration int64 `json:"OutputDuration"` // unit: ns
	}

	// Used to record statistics of additional operations, which are not included in the above stages
	OtherStage struct {
		TxnIncrStatementS3 S3Request `json:"TxnIncrStatementS3"`
	}

	// FileService(S3 or localFS) Read Data time Consumption
	IOAccessTimeConsumption int64
	// S3 FileService Prefetch File IOMerge time Consumption
	S3FSPrefetchFileIOMergerTimeConsumption int64

	// Local FileService blocking wait IOMerge time Consumption, which is included in IOAccessTimeConsumption
	LocalFSReadIOMergerTimeConsumption int64
	// S3 FileService blocking wait IOMerge time Consumption, which is included in IOAccessTimeConsumption
	S3FSReadIOMergerTimeConsumption int64

	WaitActiveCost time.Duration `json:"WaitActive"`
}

// S3Request structure is used to record the number of times each S3 operation is performed
type S3Request struct {
	List      int64 `json:"List,omitempty"`
	Head      int64 `json:"Head,omitempty"`
	Put       int64 `json:"Put,omitempty"`
	Get       int64 `json:"Get,omitempty"`
	Delete    int64 `json:"Delete,omitempty"`
	DeleteMul int64 `json:"DeleteMul,omitempty"`
}

// CountLIST return s.List.
// Diff: 1) aws/aliyun treats List as PUT; 2) tencent cloud/huaweicloud treats List as GET
// cc https://github.com/matrixorigin/MO-Cloud/issues/4175#issuecomment-2375813480
func (s S3Request) CountLIST() int64   { return s.List }
func (s S3Request) CountPUT() int64    { return s.Put }
func (s S3Request) CountGET() int64    { return s.Head + s.Get }
func (s S3Request) CountDELETE() int64 { return s.Delete + s.DeleteMul }

func (stats *StatsInfo) CompileStart() {
	if stats == nil {
		return
	}
	if !stats.CompileStage.CompileStartTime.IsZero() {
		return
	}
	stats.CompileStage.CompileStartTime = time.Now()
}

func (stats *StatsInfo) CompileEnd() {
	if stats == nil {
		return
	}
	end := time.Now()
	stats.CompileStage.CompileDuration = end.Sub(stats.CompileStage.CompileStartTime)
	reportCpuTime(stats, stats.CompileStage.CompileStartTime, end)
}

func (stats *StatsInfo) PlanStart() {
	if stats == nil {
		return
	}
	stats.PlanStage.PlanStartTime = time.Now()
}

func (stats *StatsInfo) PlanEnd() {
	if stats == nil {
		return
	}
	end := time.Now()
	stats.PlanStage.PlanDuration = end.Sub(stats.PlanStage.PlanStartTime)
	reportCpuTime(stats, stats.PlanStage.PlanStartTime, end)
}

func (stats *StatsInfo) ExecutionStart() {
	if stats == nil {
		return
	}
	stats.ExecuteStage.ExecutionStartTime = time.Now()
}

func (stats *StatsInfo) ExecutionEnd() {
	if stats == nil {
		return
	}
	stats.ExecuteStage.ExecutionEndTime = time.Now()
	stats.ExecuteStage.ExecutionDuration = stats.ExecuteStage.ExecutionEndTime.Sub(stats.ExecuteStage.ExecutionStartTime)
	reportCpuTime(stats, stats.ExecuteStage.ExecutionStartTime, stats.ExecuteStage.ExecutionEndTime)
}

func (stats *StatsInfo) AddOutputTimeConsumption(start time.Time, end time.Time) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.ExecuteStage.OutputDuration, end.Sub(start).Nanoseconds())
	reportCpuTime(stats, start, end)
}

func (stats *StatsInfo) AddBuidReaderTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.BuildReaderDuration, int64(d))
}

func (stats *StatsInfo) AddIOAccessTimeConsumption(start time.Time, end time.Time) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.IOAccessTimeConsumption, end.Sub(start).Nanoseconds())
	reportIOTime(stats, start, end)
}

func (stats *StatsInfo) AddLocalFSReadIOMergerTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.LocalFSReadIOMergerTimeConsumption, int64(d))
}
func (stats *StatsInfo) AddS3FSPrefetchFileIOMergerTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.S3FSPrefetchFileIOMergerTimeConsumption, int64(d))
}
func (stats *StatsInfo) AddS3FSReadIOMergerTimeConsumption(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.S3FSReadIOMergerTimeConsumption, int64(d))
}

func (stats *StatsInfo) ResetIOMergerTimeConsumption() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.LocalFSReadIOMergerTimeConsumption, 0)
	atomic.StoreInt64(&stats.S3FSPrefetchFileIOMergerTimeConsumption, 0)
	atomic.StoreInt64(&stats.S3FSReadIOMergerTimeConsumption, 0)
}

func (stats *StatsInfo) ResetIOAccessTimeConsumption() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.IOAccessTimeConsumption, 0)
}

func (stats *StatsInfo) ResetBuildReaderTimeConsumption() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.BuildReaderDuration, 0)
}

func (stats *StatsInfo) ResetCompilePreRunOnceDuration() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.CompilePreRunOnceDuration, 0)
}

func (stats *StatsInfo) ResetCompilePreRunOnceWaitLock() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.CompilePreRunOnceWaitLock, 0)
}

func (stats *StatsInfo) ResetScopePrepareDuration() {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.ScopePrepareDuration, 0)
}

func (stats *StatsInfo) IOMergerTimeConsumption() int64 {
	if stats == nil {
		return 0
	}
	return stats.LocalFSReadIOMergerTimeConsumption +
		stats.S3FSPrefetchFileIOMergerTimeConsumption +
		stats.S3FSReadIOMergerTimeConsumption
}

func (stats *StatsInfo) AddBuildPlanStatsConsumption(start time.Time, end time.Time) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsDuration, end.Sub(start).Nanoseconds())
	reportCpuTime(stats, start, end)
}

func (stats *StatsInfo) AddBuildPlanStatsIOConsumption(start time.Time, end time.Time) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsIOConsumption, end.Sub(start).Nanoseconds())
	reportIOTime(stats, start, end)
}

func (stats *StatsInfo) AddStatsStatsInCacheDuration(start time.Time, end time.Time) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsInCacheDuration, end.Sub(start).Nanoseconds())
	reportCpuTime(stats, start, end)
}

func (stats *StatsInfo) AddBuildPlanResolveVarConsumption(start time.Time, end time.Time) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanResolveVarDuration, end.Sub(start).Nanoseconds())
	reportCpuTime(stats, start, end)
}

func (stats *StatsInfo) AddCompileTableScanConsumption(start time.Time, end time.Time) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.CompileStage.CompileTableScanDuration, end.Sub(start).Nanoseconds())
	reportTableScanTime(stats, start, end)
}

func (stats *StatsInfo) AddBuildPlanS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.List, sreq.List)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Head, sreq.Head)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Put, sreq.Put)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Get, sreq.Get)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.Delete, sreq.Delete)
	atomic.AddInt64(&stats.PlanStage.BuildPlanS3Request.DeleteMul, sreq.DeleteMul)
	reportIOCount(stats, &sreq)
}

func (stats *StatsInfo) AddBuildPlanStatsS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.List, sreq.List)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Head, sreq.Head)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Put, sreq.Put)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Get, sreq.Get)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.Delete, sreq.Delete)
	atomic.AddInt64(&stats.PlanStage.BuildPlanStatsS3.DeleteMul, sreq.DeleteMul)
	reportIOCount(stats, &sreq)
}

func (stats *StatsInfo) AddCompileS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.List, sreq.List)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Head, sreq.Head)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Put, sreq.Put)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Get, sreq.Get)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.Delete, sreq.Delete)
	atomic.AddInt64(&stats.CompileStage.CompileS3Request.DeleteMul, sreq.DeleteMul)
	reportIOCount(stats, &sreq)
}

// CompileExpandRangesS3Request
func (stats *StatsInfo) CompileExpandRangesS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.List, sreq.List)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Head, sreq.Head)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Put, sreq.Put)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Get, sreq.Get)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.Delete, sreq.Delete)
	atomic.AddInt64(&stats.CompileStage.CompileExpandRangesS3.DeleteMul, sreq.DeleteMul)
	reportIOCount(stats, &sreq)
}

func (stats *StatsInfo) AddScopePrepareS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.List, sreq.List)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Head, sreq.Head)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Put, sreq.Put)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Get, sreq.Get)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.Delete, sreq.Delete)
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareS3Request.DeleteMul, sreq.DeleteMul)
	reportIOCount(stats, &sreq)
}

func (stats *StatsInfo) AddTxnIncrStatementS3Request(sreq S3Request) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.List, sreq.List)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Head, sreq.Head)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Put, sreq.Put)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Get, sreq.Get)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.Delete, sreq.Delete)
	atomic.AddInt64(&stats.OtherStage.TxnIncrStatementS3.DeleteMul, sreq.DeleteMul)
	reportIOCount(stats, &sreq)
}

func (stats *StatsInfo) AddScopePrepareDuration(d int64) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.ScopePrepareDuration, d)
}

// NOTE: CompilePreRunOnceDuration is a one-time statistic and does not require accumulation
func (stats *StatsInfo) StoreCompilePreRunOnceDuration(d time.Duration) {
	if stats == nil {
		return
	}
	atomic.StoreInt64(&stats.PrepareRunStage.CompilePreRunOnceDuration, int64(d))
}

func (stats *StatsInfo) AddPreRunOnceWaitLockDuration(d int64) {
	if stats == nil {
		return
	}
	atomic.AddInt64(&stats.PrepareRunStage.CompilePreRunOnceWaitLock, d)
}

//--------------------------------------------------------------------------------------------------------------

func (stats *StatsInfo) SetWaitActiveCost(cost time.Duration) {
	if stats == nil {
		return
	}
	stats.WaitActiveCost = cost
}

// Reset StatsInfo into zero state
func (stats *StatsInfo) Reset() {
	if stats == nil {
		return
	}
	if stats.Metadata != nil {
		stats.Metadata.Lock()
		defer stats.Metadata.Unlock()
	}
	*stats = StatsInfo{}
}

func (stats *StatsInfo) Report() {
	if stats.Metadata != nil {
		stats.Metadata.Lock()
		defer stats.Metadata.Unlock()
	}

}

func ContextWithStatsInfo(requestCtx context.Context, stats *StatsInfo) context.Context {
	return context.WithValue(requestCtx, statsInfoKey{}, stats)
}

func StatsInfoFromContext(requestCtx context.Context) *StatsInfo {
	if requestCtx == nil {
		return nil
	}
	if stats, ok := requestCtx.Value(statsInfoKey{}).(*StatsInfo); ok {
		return stats
	}
	return nil
}

// EnsureStatsInfoCanBeFound ensure a statement statistic is set in context, if not, copy one from another context, this function is copied from EnsureStatementProfiler
func EnsureStatsInfoCanBeFound(ctx context.Context, from context.Context) context.Context {
	if v := ctx.Value(statsInfoKey{}); v != nil {
		// already set
		return ctx
	}
	v := from.Value(statsInfoKey{})
	if v == nil {
		// not set in from
		return ctx
	}
	ctx = context.WithValue(ctx, statsInfoKey{}, v)
	return ctx
}

type StatsType string

func (s StatsType) String() string { return string(s) }

const (
	CpuType        StatsType = "cpu"
	MemoryTimeType           = "memory_time"
	IOTime                   = "iotime"
	S3IOIn                   = "s3ioin"
	S3IOOut                  = "s3ioout"
	S3List                   = "s3list"
	S3Head                   = "s3head"
	S3Put                    = "s3put"
	S3Get                    = "s3get"
	S3Delete                 = "s3delete"
	S3DeleteMul              = "s3deleteMul"
	TableScanType            = "table_scan"
)

type ReportTimeRange func(stats *StatsInfo, info StatsType, start, end time.Time)
type ReportResource func(stats *StatsInfo, info StatsType, ts time.Time, value int64)

func SetCpuReporter(f ReportTimeRange, fr ReportResource) {
	reportStatementTimeConsume = f
	reportStatementResource = fr
}

var reportStatementTimeConsume ReportTimeRange = func(stats *StatsInfo, typ StatsType, start, end time.Time) {}

var reportStatementResource ReportResource = func(stats *StatsInfo, typ StatsType, ts time.Time, value int64) {}

func reportIOCount(stats *StatsInfo, sreq *S3Request) {
	if stats.EnableCollect {
		end := time.Now()
		reportStatementResource(stats, S3List, end, sreq.List)
		reportStatementResource(stats, S3Head, end, sreq.Head)
		reportStatementResource(stats, S3Put, end, sreq.Put)
		reportStatementResource(stats, S3Get, end, sreq.Get)
		reportStatementResource(stats, S3Delete, end, sreq.Delete)
		reportStatementResource(stats, S3DeleteMul, end, sreq.DeleteMul)
	}
}

func reportIOTime(stats *StatsInfo, start, end time.Time) {
	if stats.EnableCollect {
		reportStatementTimeConsume(stats, IOTime, start, end)
	}
}

func reportCpuTime(stats *StatsInfo, start, end time.Time) {
	if stats.EnableCollect {
		reportStatementTimeConsume(stats, CpuType, start, end)
	}
}

func reportTableScanTime(stats *StatsInfo, start, end time.Time) {
	if stats.EnableCollect {
		reportStatementTimeConsume(stats, TableScanType, start, end)
	}
}
