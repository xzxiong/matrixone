package main

import (
	"bufio"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"io"
	"math/rand"
	"os"
	"runtime/pprof"
)

func startCPUProfile() func() {
	cpuProfilePath := ""
	if cpuProfilePath == "" {
		cpuProfilePath = "cpu-profile"
	}
	f, err := os.Create(cpuProfilePath)
	if err != nil {
		panic(err)
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		panic(err)
	}
	logutil.Infof("CPU profiling enabled, writing to %s", cpuProfilePath)
	return func() {
		pprof.StopCPUProfile()
		f.Close()
	}
}

func writeHeapProfile() {
	profile := pprof.Lookup("heap")
	if profile == nil {
		return
	}
	profilePath := ""
	if profilePath == "" {
		profilePath = "heap-profile"
	}
	f, err := os.Create(profilePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := profile.WriteTo(f, 0); err != nil {
		panic(err)
	}
	logutil.Infof("Heap profile written to %s", profilePath)
}

const fileName = "execPlan.bp"

var load = false

var cnt int64

var arr []byte

func main() {

	if len(os.Args) >= 2 && os.Args[1] == "true" {
		load = true
		logutil.Infof("do load")
	}

	//stop := startCPUProfile()
	//defer stop()
	//defer writeHeapProfile()

	var queue []*plan2.Plan
	if load {
		queue = doLoad(queue)
	}
	logutil.Infof("read len(queue): %d", len(queue))

	if load {
		go func() {
			for {
				idx := rand.Int63() % int64(len(queue))
				logutil.Infof("%v", queue[idx].String())
			}
		}()
	} else {
		arr = make([]byte, 256<<20)
		go func() {
			for {
				idx := rand.Int63() % int64(len(arr))
				logutil.Infof("%v", arr[idx])
			}
		}()
	}

	for i := 0; i < 6; i++ {
		go doInc()
	}
	doInc()
	//start := time.Now()
	//for time.Now().Sub(start) < 3*time.Second {
	//	doInc()
	//	//runtime.Gosched()
	//}
	logutil.Infof("do cnt: %d, len(queue): %d", cnt, len(queue))
	if load {
		idx := rand.Int63() % int64(len(queue))
		logutil.Infof("%v", queue[idx].String())
	}
}

func doInc() {
	var i int64
	for {
		i++
		cnt += i
		if i%8192 == 0 {
			p := &plan2.Plan_Query{Query: &plan2.Query{
				StmtType: 0,
				Steps:    nil,
				Nodes:    nil,
				Params:   nil,
				Headings: nil,
				LoadTag:  false,
			}}
			p.Query.Nodes = append(p.Query.Nodes, &plan2.Node{
				NodeType:        0,
				NodeId:          1,
				Stats:           nil,
				Children:        nil,
				ProjectList:     nil,
				JoinType:        0,
				OnList:          nil,
				BuildOnLeft:     false,
				FilterList:      nil,
				GroupBy:         nil,
				GroupingSet:     nil,
				AggList:         nil,
				WinSpec:         nil,
				OrderBy:         nil,
				Limit:           nil,
				Offset:          nil,
				UpdateCtx:       nil,
				TableDef:        nil,
				TableDefVec:     nil,
				ObjRef:          nil,
				RowsetData:      nil,
				ExtraOptions:    "",
				DeleteCtx:       nil,
				BindingTags:     nil,
				AnalyzeInfo:     nil,
				TblFuncExprList: nil,
				Parallelism:     0,
				ClusterTable:    nil,
				NotCacheable:    false,
				InsertCtx:       nil,
				CurrentStep:     0,
				SourceStep:      0,
				BlockFilterList: nil,
			})
			logutil.Infof("%v", p)
		}
	}
}

func doLoad(queue []*plan2.Plan) []*plan2.Plan {

	var err error
	var f *os.File
	var r *bufio.Reader
	var sizeByte [4]byte
	var n int

	f, err = os.Open(fileName) //创建文件
	if err != nil {
		logutil.Errorf("create file fail: %s", err)
		return nil
	}
	r = bufio.NewReader(f)
	for {
		n, err = io.ReadFull(r, sizeByte[:])
		if err == io.EOF {
			logutil.Infof("read cnt: %d", cnt)
			break
		}
		if n != 4 {
			logutil.Errorf("read size length: %d != %d", n, 4)
			break
		}

		length := buf.Byte2Int(sizeByte[:])
		elem := make([]byte, length)
		n, err = io.ReadFull(r, elem[:])
		if err == io.EOF {
			logutil.Infof("read EOF, failed.")
			break
		}
		if n != length {
			logutil.Errorf("read size length: %d != %d", n, length)
			break
		}

		p := &plan2.Plan{}
		err = p.Unmarshal(elem[:])
		if err != nil {
			logutil.Errorf("unmarshal fail: %s", err)
			break
		}
		queue = append(queue, p)

	}
	return queue
}
