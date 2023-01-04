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

package export

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/stretchr/testify/assert"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var syncBenchmarkLock sync.Mutex

const benchmarkDataPath = "benchmark/query_json"

var _ table.PathBuilder = (*BenchmarkJsonQueryPathBuilder)(nil)

type BenchmarkJsonQueryPathBuilder struct {
	table.AccountDatePathBuilder
}

// BuildETLPath implement PathBuilder
// like: benchmark/query_json/*
func (b *BenchmarkJsonQueryPathBuilder) BuildETLPath(db, name, account string) string {
	etlDirectory := benchmarkDataPath
	etlFilename := "*"
	return path.Join("/", etlDirectory, etlFilename)
}

var dummyStatsColumn = table.Column{Name: "stats", Type: "JSON", Default: "{}", Comment: "json fill with key: int64, float64"}

var dummyJsonTable = table.Table{
	Account:            "sys",
	Database:           "test",
	Table:              "dummy_json",
	Columns:            []table.Column{dummyInt64Column, dummyFloat64Column, dummyStatsColumn},
	PrimaryKeyColumn:   nil,
	Engine:             table.ExternalTableEngine,
	Comment:            "",
	PathBuilder:        &BenchmarkJsonQueryPathBuilder{},
	AccountColumn:      nil,
	TableOptions:       nil,
	SupportUserAccess:  false,
	SupportConstAccess: false,
}

type dummyJsonItem struct {
	i int64
	f float64
}

func (i *dummyJsonItem) CsvField(row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(dummyInt64Column, fmt.Sprintf("%d", i.i))
	row.SetColumnVal(dummyFloat64Column, fmt.Sprintf("%f", i.f))
	m := make(map[string]any)
	m["int64"] = i.i
	m["float64"] = i.f
	row.SetColumnVal(dummyStatsColumn, string(json.MustMarshal(&m)))
	return row.ToStrings()
}

var initOnce sync.Once

func prepareGenCsv(b *testing.B, N int) {
	b.Logf("prepareGenCsv do, N: %d", N)
	defer b.Logf("prepareGenCsv done")

	ctx := context.TODO()
	filePath := path.Join(`../../../mo-data/etl/`, benchmarkDataPath, `dummy.csv`)
	buf := new(bytes.Buffer)
	row := dummyJsonTable.GetRow(ctx)
	for i := 0; i < N; i++ {
		i := dummyJsonItem{
			i: int64(i),
			f: float64(i),
		}
		dummyWriteCsvOneLine(ctx, buf, i.CsvField(row))
	}
	if absPath, err := filepath.Abs(filePath); err != nil {
		panic(fmt.Sprintf("create folder failed: %v", err))
	} else {
		b.Logf("path: %v", absPath)
	}
	if err := os.MkdirAll(path.Dir(filePath), fs.ModePerm); err != nil {
		panic(fmt.Sprintf("create folder failed: %v", err))
	} else if err = os.WriteFile(filePath, buf.Bytes(), fs.ModePerm); err != nil {
		panic(fmt.Sprintf("wirte file failed: %v", err))
	}

	db, err := dummySetConn(b, "127.0.0.1", 6001, "dump", "111", "")
	assert.Nil(b, err)
	_, err = db.Exec("create database if not exists `test`")
	assert.Nil(b, err)
	sql := dummyJsonTable.ToCreateSql(ctx, true)
	b.Logf("create table: %s", sql)
	_, err = db.Exec(sql)
	assert.Nil(b, err)
}

func dummySetConn(b *testing.B, host string, port int, user string, passwd string, db string) (*sql.DB, error) {
	timeo := 30 * time.Second
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local&readTimeout=%s&writeTimeout=%s&timeout=%s",
		user, passwd, host, port, db, timeo, timeo, timeo) //本地时区
	dbConn, err := dummyCreateDbConn(b, dsn, true, 3)
	if err != nil {
		return nil, err
	}
	return dbConn, nil
}

// 至少ping一次
func dummyCreateDbConn(b *testing.B, dsn string, long_conn bool, ping_cnt int) (conn *sql.DB, err error) {

	if conn, err = sql.Open("mysql", dsn); err != nil { //创建db
		b.Logf("open dsn %s failed: %s", dsn, err)
		return nil, err
	}
	for i := 0; ; {
		if err = conn.Ping(); err == nil {
			return conn, nil
		}
		if i++; i >= ping_cnt {
			b.Errorf("Ping failed over %d time(s)", ping_cnt)
			return nil, err
		}
		b.Logf("Ping %d time(s): %s, try again", i, err)
		time.Sleep(time.Second * 3)
	}

	return
}

func checkTestConfig() bool {
	_, err := os.ReadFile("need_singleton_mo")
	if err != nil {
		return false
	}
	return true
}

func dummyWriteCsvOneLine(ctx context.Context, buf *bytes.Buffer, fields []string) {
	opts := table.CommonCsvOptions
	for idx, field := range fields {
		if idx > 0 {
			buf.WriteRune(opts.FieldTerminator)
		}
		if strings.ContainsRune(field, opts.FieldTerminator) || strings.ContainsRune(field, opts.EncloseRune) || strings.ContainsRune(field, opts.Terminator) {
			buf.WriteRune(opts.EncloseRune)
			dummyQuoteFieldFunc(ctx, buf, field, opts.EncloseRune)
			buf.WriteRune(opts.EncloseRune)
		} else {
			buf.WriteString(field)
		}
	}
	buf.WriteRune(opts.Terminator)
}

var dummyQuoteFieldFunc = func(ctx context.Context, buf *bytes.Buffer, value string, enclose rune) string {
	replaceRules := map[rune]string{
		'"':  `""`,
		'\'': `\'`,
	}
	quotedClose, hasRule := replaceRules[enclose]
	if !hasRule {
		panic(moerr.NewInternalError(ctx, "not support csv enclose: %c", enclose))
	}
	for _, c := range value {
		if c == enclose {
			buf.WriteString(quotedClose)
		} else {
			buf.WriteRune(c)
		}
	}
	return value
}

func queryInt64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select int64 from test.dummy_json where int64 = ?;`
	count := 0
	//for i := 0; i < N; i++ {
	rows, err := db.QueryContext(ctx, sql, i)
	if err != nil {
		return count, err
	}
	for rows.Next() {
		rows.Scan(&val)
		if val != int64(i) {
			return 0, moerr.NewInternalError(ctx, "read error: %v", err)
		}
		count++
	}
	//}
	return count, nil
}

func queryFloat64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val float64
	const sql = `select float64 from test.dummy_json where float64 = ?;`
	count := 0
	//for i := 0; i < N; i++ {
	rows, err := db.QueryContext(ctx, sql, i)
	if err != nil {
		return count, err
	}
	for rows.Next() {
		rows.Scan(&val)
		if val != float64(i) {
			return 0, moerr.NewInternalError(ctx, "read error: %v", err)
		}
		count++
	}
	//}
	return count, nil
}

func queryJsonInt64ByInt64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select JSON_UNQUOTE(json_extract(stats, '$.int64'))  from test.dummy_json where int64  = ?;`
	count := 0
	//for i := 0; i < N; i++ {
	rows, err := db.QueryContext(ctx, sql, i)
	if err != nil {
		return count, err
	}
	for rows.Next() {
		rows.Scan(&val)
		if val != int64(i) {
			return 0, moerr.NewInternalError(ctx, "read error: %v", err)
		}
		count++
	}
	//}
	return count, nil
}

func queryJsonInt64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select JSON_UNQUOTE(json_extract(stats, '$.int64'))  from test.dummy_json where JSON_UNQUOTE(json_extract(stats, '$.int64'))  = ?;`
	count := 0
	//for i := 0; i < N; i++ {
	rows, err := db.QueryContext(ctx, sql, i)
	if err != nil {
		return count, err
	}
	for rows.Next() {
		rows.Scan(&val)
		if val != int64(i) {
			return 0, moerr.NewInternalError(ctx, "read error: %v", err)
		}
		count++
	}
	//}
	return count, nil
}
func queryJsonFloat64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val float64
	const sql = `select JSON_UNQUOTE(json_extract(stats, '$.float64'))  from test.dummy_json where JSON_UNQUOTE(json_extract(stats, '$.float64'))  = ?;`
	count := 0
	//for i := 0; i < N; i++ {
	rows, err := db.QueryContext(ctx, sql, i)
	if err != nil {
		return count, err
	}
	for rows.Next() {
		rows.Scan(&val)
		if val != float64(i) {
			return 0, moerr.NewInternalError(ctx, "read error: %v", err)
		}
		count++
	}
	//}
	return count, nil
}

func BenchmarkQuery10wRows(b *testing.B) {
	syncBenchmarkLock.Lock()
	defer syncBenchmarkLock.Unlock()

	type args struct {
		action func(ctx context.Context, db *sql.DB, val int) (int, error)
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "queryInt64",
			args: args{action: queryInt64},
		},
		{
			name: "queryFloat64",
			args: args{action: queryFloat64},
		},
		{
			name: "queryJsonInt64",
			args: args{action: queryJsonInt64},
		},
		{
			name: "queryJsonFloat64",
			args: args{action: queryJsonFloat64},
		},
	}

	ctx := context.TODO()
	if exist := checkTestConfig(); !exist {
		b.Skip()
	}
	b.Logf("N: %d", b.N)
	prepareGenCsv(b, 1e6)

	db, err := dummySetConn(b, "127.0.0.1", 6001, "dump", "111", "")
	assert.Nil(b, err)

	b.ResetTimer()
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cnt, err := tt.args.action(ctx, db, i)
				assert.Nil(b, err)
				assert.Equal(b, 1, cnt)
			}
		})
	}
}

func BenchmarkQuery1kRows(b *testing.B) {
	syncBenchmarkLock.Lock()
	defer syncBenchmarkLock.Unlock()

	type args struct {
		action func(ctx context.Context, db *sql.DB, val int) (int, error)
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "queryInt64",
			args: args{action: queryInt64},
		},
		{
			name: "queryFloat64",
			args: args{action: queryFloat64},
		},
		{
			name: "queryJsonInt64",
			args: args{action: queryJsonInt64},
		},
		{
			name: "queryJsonFloat64",
			args: args{action: queryJsonFloat64},
		},
		{
			name: "queryJsonInt64ByInt64",
			args: args{action: queryJsonInt64ByInt64},
		},
	}

	ctx := context.TODO()
	if exist := checkTestConfig(); !exist {
		b.Skip()
	}
	b.Logf("N: %d", b.N)
	prepareGenCsv(b, 1e4)

	db, err := dummySetConn(b, "127.0.0.1", 6001, "dump", "111", "")
	assert.Nil(b, err)

	b.ResetTimer()
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cnt, err := tt.args.action(ctx, db, i)
				assert.Nil(b, err)
				assert.Equal(b, 1, cnt)
			}
		})
	}
}
