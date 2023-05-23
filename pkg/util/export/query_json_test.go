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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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

var dummyStatsColumn = table.JsonColumn("stats", "json fill with key: int64, float64")
var dummyStatsStrColumn = table.TextColumn("stats_str", "json-str fill with key: int64, float64")
var dummyStatsArrColumn = table.TextColumn("stats_arr", "json-str int64 array")
var dummyStatsArr16Column = table.TextColumn("stats_arr_16", "json-str int64 array with 16 elems")
var dummyStatsShortColumn = table.TextColumn("stats_str_short", "json-str int64 array, with short key")

var dummyJsonTable = &table.Table{
	Account:            "sys",
	Database:           "test",
	Table:              "dummy_json",
	Columns:            []table.Column{dummyInt64Column, dummyFloat64Column, dummyStatsColumn, dummyStatsStrColumn, dummyStatsArrColumn, dummyStatsArr16Column, dummyStatsShortColumn},
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

func (i *dummyJsonItem) FillRow(row *table.Row) {
	row.Reset()
	row.SetColumnVal(dummyInt64Column, table.Int64Field(i.i))
	row.SetColumnVal(dummyFloat64Column, table.Float64Field(i.f))
	m := make(map[string]any)
	s := ` {"statistics": {"IO": [{"name": "Disk IO", "unit": "byte", "value": 0}, {"name": "S3 IO Byte", "unit": "byte", "value": 224}, {"name": "S3 IO Input Count", "unit": "count", "value": 0}, {"name": "S3 IO Output Count", "unit": "count", "value": 0}], "Memory": [{"name": "Memory Size", "unit": "byte", "value": 225}], "Network": [{"name": "Network", "unit": "byte", "value": 0}], "Throughput": [{"name": "Input Rows", "unit": "count", "value": 1}, {"name": "Output Rows", "unit": "count", "value": 0}, {"name": "Input Size", "unit": "byte", "value": 224}, {"name": "Output Size", "unit": "byte", "value": 0}], "Time": [{"name": "Time Consumed", "unit": "ns", "value": 39833}, {"name": "Wait Time", "unit": "ns", "value": 280250}]}, "totalStats": {"name": "Time spent", "unit": "ns", "value": 39833}}`
	json.MustUnmarshal([]byte(s), &m)
	m["int64"] = i.i
	m["float64"] = i.f
	jsonStr := json.MustMarshal(&m)
	row.SetColumnVal(dummyStatsColumn, table.StringField(string(jsonStr)))
	row.SetColumnVal(dummyStatsStrColumn, table.StringField(string(jsonStr)))
	// [16] array
	slice := [16]int64{}
	for i := 0; i < 16; i++ {
		slice[i] = rand.Int63()
	}
	// format [5]arr
	arr := fmt.Sprintf("[1, %d, %d, %d, %d]", i.i, slice[2], slice[3], slice[4])
	row.SetColumnVal(dummyStatsArrColumn, table.StringField(arr))
	// format [16]arr
	slice[0], slice[1] = 1, i.i
	row.SetColumnVal(dummyStatsArr16Column, table.StringField(string(json.MustMarshal(&slice))))
	// json like: `{"int64":?, "cpu":1, "mem":2, "s3in": 123, "s3out":123}`
	shortM := make(map[string]int64)
	shortM["int64"] = i.i
	shortM["cpu"] = slice[2]
	shortM["mem"] = slice[3]
	shortM["s3in"] = 0
	shortM["s3out"] = 0
	row.SetColumnVal(dummyStatsShortColumn, table.StringField(string(json.MustMarshal(&shortM))))
}

func (i *dummyJsonItem) CsvField(row *table.Row) []string {
	i.FillRow(row)
	return row.ToStrings()
}

var initOnce sync.Once

func prepareGenCsv(b *testing.B, N int) {
	b.Logf("prepareGenCsv do, N: %d", N)
	defer b.Logf("prepareGenCsv done")

	ctx := context.TODO()
	filePath := path.Join(`../../../mo-data/etl/`, benchmarkDataPath, `dummy`)
	removeFileAllExtension(b, filePath)
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

func prepareGenTae(b *testing.B, N int) {
	b.Logf("prepareGenCsv do, N: %d", N)
	defer b.Logf("prepareGenCsv done")

	err, ctx := generateTae(b, N, `dummy`)

	db, err := dummySetConn(b, "127.0.0.1", 6001, "dump", "111", "")
	assert.Nil(b, err)
	_, err = db.Exec("create database if not exists `test`")
	assert.Nil(b, err)
	sql := dummyJsonTable.ToCreateSql(ctx, true)
	b.Logf("create table: %s", sql)
	_, err = db.Exec(sql)
	assert.Nil(b, err)
}

func generateTae(b *testing.B, N int, filename string) (error, context.Context) {

	rootDir := `../../../mo-data/etl/`
	absPath, err := filepath.Abs(rootDir)
	if err != nil {
		panic(fmt.Sprintf("get abs path failed: %v", err))
	}
	b.Logf("path: %v", absPath)

	mp, err := mpool.NewMPool("test_etl_fs_writer", 0, mpool.NoFixed)
	assert.Nil(b, err)
	fservice, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, absPath)
	assert.Nil(b, err)

	ctx := context.TODO()
	filePath := path.Join(benchmarkDataPath, filename)
	removeFileAllExtension(b, path.Join(absPath, filePath))
	filePath = filePath + table.TaeExtension

	writer := etl.NewTAEWriter(ctx, dummyJsonTable, mp, filePath, fservice)
	for i := 0; i < N; i++ {
		row := dummyJsonTable.GetRow(ctx)
		i := dummyJsonItem{
			i: int64(i),
			f: float64(i),
		}
		i.FillRow(row)
		writer.WriteRow(row)
	}
	_, err = writer.FlushAndClose()
	assert.Nil(b, err)

	return err, ctx
}

func removeFileAllExtension(b *testing.B, path string) {
	files := []string{path, path + table.TaeExtension, path + table.CsvExtension}
	for _, fp := range files {
		err := os.Remove(fp)
		if err != nil {
			if e := err.(*os.PathError); e.Err != syscall.ENOENT {
				assert.Nil(b, err)
			}
		}
	}
}

func readTae(b *testing.B, filename string) {
	filename = filename + table.TaeExtension
	rootDir := `../../../mo-data/etl/`
	absPath, err := filepath.Abs(rootDir)
	if err != nil {
		panic(fmt.Sprintf("get abs path failed: %v", err))
	}
	b.Logf("path: %v", absPath)

	mp, err := mpool.NewMPool("test_etl_fs_writer", 0, mpool.NoFixed)
	assert.Nil(b, err)
	fservice, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, absPath)
	assert.Nil(b, err)

	ctx := context.TODO()
	filePath := path.Join(benchmarkDataPath, filename)

	entrys, err := fservice.List(context.TODO(), "etl:/"+benchmarkDataPath)
	require.Nil(b, err)
	if len(entrys) == 0 {
		b.Skip()
	}
	require.Equal(b, 1, len(entrys))
	require.Equal(b, filename, entrys[0].Name)

	fileSize := entrys[0].Size

	r, err := etl.NewTaeReader(ctx, dummyJsonTable, filePath, fileSize, fservice, mp)

	// read data
	batchs, err := r.ReadAll(ctx)
	require.Nil(b, err)

	readCnt := 0
	for batIDX, bat := range batchs {
		for _, vec := range bat.Vecs {
			rows, err := etl.GetVectorArrayLen(context.TODO(), vec)
			require.Nil(b, err)
			b.Logf("calculate length: %d, vec.Length: %d, type: %s", rows, vec.Length(), vec.GetType().String())
		}
		rows := bat.Vecs[0].Length()
		ctn := strings.Builder{}
		for rowId := 0; rowId < rows; rowId++ {
			for _, vec := range bat.Vecs {
				val, err := etl.ValToString(context.TODO(), vec, rowId)
				require.Nil(b, err)
				ctn.WriteString(val)
				ctn.WriteString(",")
			}
			ctn.WriteRune('\n')
		}
		b.Logf("batch %d: \n%s", batIDX, ctn.String())
		//t.Logf("read batch %d", batIDX)
		readCnt += rows
	}
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

func queryExist(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val string
	const sql = `select __mo_filepath from test.dummy_json limit 1;`
	count := 0
	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return count, err
	}
	for rows.Next() {
		err = rows.Scan(&val)
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func queryInt64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select int64 from test.dummy_json where int64 = ?;`
	count := 0
	rows, err := db.QueryContext(ctx, sql, i)
	if err != nil {
		return count, err
	}
	for rows.Next() {
		err = rows.Scan(&val)
		if err != nil {
			return 0, err
		}
		if val != int64(i) {
			return 0, moerr.NewInternalError(ctx, "read error: %v", err)
		}
		count++
	}
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
	return count, nil
}
func queryJsonFloat64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val float64
	const sql = `select JSON_UNQUOTE(json_extract(stats, '$.float64'))  from test.dummy_json where JSON_UNQUOTE(json_extract(stats, '$.float64'))  = ?;`
	count := 0
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
	return count, nil
}
func queryJsonStrInt64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select JSON_UNQUOTE(json_extract(stats_str, '$.int64'))  from test.dummy_json where JSON_UNQUOTE(json_extract(stats_str, '$.int64'))  = ?;`
	count := 0
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
	return count, nil
}
func queryJsonArrInt64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select JSON_UNQUOTE(json_extract(stats_arr, '$[1]'))  from test.dummy_json where JSON_UNQUOTE(json_extract(stats_arr, '$[1]'))  = ?;`
	count := 0
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
	return count, nil
}
func queryJsonArr16Int64(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select JSON_UNQUOTE(json_extract(stats_arr_16, '$[1]'))  from test.dummy_json where JSON_UNQUOTE(json_extract(stats_arr_16, '$[1]'))  = ?;`
	count := 0
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
	return count, nil
}
func queryJsonStrShortKey(ctx context.Context, db *sql.DB, i int) (int, error) {
	var val int64
	const sql = `select JSON_UNQUOTE(json_extract(stats_str_short, '$.int64'))  from test.dummy_json where JSON_UNQUOTE(json_extract(stats_str_short, '$.int64'))  = ?;`
	count := 0
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
		{
			name: "queryJsonStrInt64",
			args: args{action: queryJsonStrInt64},
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

func BenchmarkQueryTae10wRows(b *testing.B) {
	syncBenchmarkLock.Lock()
	defer syncBenchmarkLock.Unlock()

	type args struct {
		action func(ctx context.Context, db *sql.DB, val int) (int, error)
	}
	tests := []struct {
		name    string
		execCnt int
		args    args
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
		{
			name: "queryJsonStrInt64",
			args: args{action: queryJsonStrInt64},
		},
		{
			name: "queryJsonArrInt64",
			args: args{action: queryJsonArrInt64},
		},
		{
			name: "queryJsonArr16Int64",
			args: args{action: queryJsonArr16Int64},
		},
		{
			name: "queryJsonStrShortKey",
			args: args{action: queryJsonStrShortKey},
		},
	}

	ctx := context.TODO()
	if exist := checkTestConfig(); !exist {
		b.Skip()
	}
	b.Logf("N: %d", b.N)
	prepareGenTae(b, 1e6)

	db, err := dummySetConn(b, "127.0.0.1", 6001, "dump", "111", "")
	assert.Nil(b, err)
	err = db.Ping()
	assert.Nil(b, err)

	b.ResetTimer()
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if tt.execCnt > 0 && i >= tt.execCnt {
					break
				}
				cnt, err := tt.args.action(ctx, db, i)
				assert.Nil(b, err)
				assert.Equal(b, 1, cnt)
			}
		})
	}
}

func BenchmarkQueryTae1kRows(b *testing.B) {
	syncBenchmarkLock.Lock()
	defer syncBenchmarkLock.Unlock()

	type args struct {
		action func(ctx context.Context, db *sql.DB, val int) (int, error)
	}
	tests := []struct {
		name    string
		execCnt int
		args    args
	}{
		//{
		//	name:    "queryExist",
		//	args:    args{action: queryExist},
		//	execCnt: 1,
		//},
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
		{
			name: "queryJsonStrInt64",
			args: args{action: queryJsonStrInt64},
		},
		{
			name: "queryJsonArrInt64",
			args: args{action: queryJsonArrInt64},
		},
		{
			name: "queryJsonArr16Int64",
			args: args{action: queryJsonArr16Int64},
		},
		{
			name: "queryJsonStrShortKey",
			args: args{action: queryJsonStrShortKey},
		},
	}

	ctx := context.TODO()
	if exist := checkTestConfig(); !exist {
		b.Skip()
	}
	b.Logf("N: %d", b.N)
	prepareGenTae(b, 1e4)

	db, err := dummySetConn(b, "127.0.0.1", 6001, "dump", "111", "")
	assert.Nil(b, err)
	err = db.Ping()
	assert.Nil(b, err)

	b.ResetTimer()
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if tt.execCnt > 0 && i >= tt.execCnt {
					break
				}
				cnt, err := tt.args.action(ctx, db, i)
				assert.Nil(b, err)
				assert.Equal(b, 1, cnt)
			}
		})
	}
}

func BenchmarkWriteRead(b *testing.B) {
	syncBenchmarkLock.Lock()
	defer syncBenchmarkLock.Unlock()

	rows := int(1e4)
	filename := `dummy`
	generateTae(b, rows, filename)
	readTae(b, filename)

}
