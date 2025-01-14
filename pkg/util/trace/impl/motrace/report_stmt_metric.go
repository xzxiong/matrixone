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

package motrace

import (
	"context"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

type StatementMetric struct {
	Account              string    `json:"account"`
	StatementFingerprint string    `json:"statement_fingerprint"`
	StatementTemplateId  string    `json:"statement_template_id"`
	MetricName           string    `json:"metric_name"`
	Timestamp            time.Time `json:"timestamp"`
	Value                float64   `json:"value"`

	aggrCount int64
}

func (s *StatementMetric) GetName() string {
	return SingleStatementMetricTable.GetName()
}

// Before implements table.WindowKey
func (s *StatementMetric) Before(end time.Time) bool {
	return s.Timestamp.Before(end)
}

// Key implements table.Item
func (s *StatementMetric) Key(duration time.Duration) table.WindowKey {
	return s
}

// Aggred implements table.Item
func (s *StatementMetric) Aggred() int64 { return s.aggrCount }

func (s *StatementMetric) Size() int64 {
	num := int64(unsafe.Sizeof(s)) + int64(
		len(s.Account)+len(s.StatementFingerprint)+len(s.StatementTemplateId),
	)
	return num
}

func (s *StatementMetric) Free() {
	s.Account = ""
	s.StatementFingerprint = ""
	s.StatementTemplateId = ""
	s.Value = 0
	s.aggrCount = 0
}

func (s *StatementMetric) GetTable() *table.Table { return SingleStatementMetricTable }

func (s *StatementMetric) FillRow(ctx context.Context, row *table.Row) {
	row.Reset()
	row.SetColumnVal(accountCol, table.StringField(s.Account))
	row.SetColumnVal(stmtFgCol, table.StringField(s.StatementFingerprint))
	row.SetColumnVal(stmtTmpIdCol, table.StringField(s.StatementTemplateId))
	row.SetColumnVal(nodeUUIDCol, table.StringField(GetNodeResource().NodeUuid))
	row.SetColumnVal(nodeTypeCol, table.StringField(GetNodeResource().NodeType))
	row.SetColumnVal(timestampCol, table.TimeField(s.Timestamp))
	row.SetColumnVal(valueCol, table.Float64Field(s.Value))
	row.SetColumnVal(metricNameCol, table.StringField(s.MetricName))
}

type StatementMetricAggregator struct{}

func (a StatementMetricAggregator) NewFunc(i table.Item, ctx context.Context) table.Item {
	if s, ok := i.(*StatementMetric); ok {
		return s
	}
	return nil
}

func (a StatementMetricAggregator) UpdateFunc(ctx context.Context, existing, new table.Item) {
	e := existing.(*StatementMetric)
	n := new.(*StatementMetric)
	e.aggrCount++
	e.Value += n.Value
}

func (a StatementMetricAggregator) FilterFunc(i table.Item) bool {
	_, ok := i.(*StatementMetric)

	if !ok {
		return false
	}

	return true
}
func ReportStatementCpu(stats *statistic.StatsInfo, typ statistic.StatsType, start, end time.Time) {
	if !GetTracerProvider().IsEnable() {
		return
	}

	// TODO: async op
	// generate multi StatementMetric records.
	window := GetTracerProvider().aggregationWindow
	lastWindowBegin := end.Truncate(window)
	lastWindowEnd := lastWindowBegin.Add(window)

	ctx := context.Background()
	current := start
	for current.Before(lastWindowBegin) {
		currentEnd := current.Truncate(window).Add(window)
		GetGlobalBatchProcessor().Collect(ctx, &StatementMetric{
			Account:              stats.Metadata.Account,
			StatementFingerprint: stats.Metadata.StatementFingerprint,
			StatementTemplateId:  stats.Metadata.StatementTemplateId,
			MetricName:           typ.String(),
			Timestamp:            currentEnd,
			Value:                float64(currentEnd.Sub(current).Nanoseconds()),
			aggrCount:            1,
		})
		current = currentEnd
	}
	GetGlobalBatchProcessor().Collect(ctx, &StatementMetric{
		Account:              stats.Metadata.Account,
		StatementFingerprint: stats.Metadata.StatementFingerprint,
		StatementTemplateId:  stats.Metadata.StatementTemplateId,
		MetricName:           typ.String(),
		Timestamp:            lastWindowEnd,
		Value:                float64(end.Sub(current).Nanoseconds()),
		aggrCount:            1,
	})

}

var ReportStatementMem = func(ctx context.Context, stats *statistic.StatsInfo, typ statistic.StatsType, start, end time.Time, value int64 /*byte*/) {
	if !GetTracerProvider().IsEnable() {
	}

	// generate multi StatementMetric records.

	GetGlobalBatchProcessor().Collect(ctx, nil)
}
