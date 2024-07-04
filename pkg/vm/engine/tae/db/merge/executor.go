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

package merge

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

type activeTaskStats map[uint64]struct {
	blk      int
	estBytes int
}

// MergeExecutor consider resources to decide to merge or not.
type MergeExecutor struct {
	tableName           string
	rt                  *dbutils.Runtime
	cnSched             CNMergeScheduler
	memAvail            int
	memSpare            int // 10% of total memory or container memory limit
	transPageLimit      uint64
	cpuPercent          float64
	activeMergeBlkCount int32
	activeEstimateBytes int64
	roundMergeRows      uint64
	taskConsume         struct {
		sync.Mutex
		o map[objectio.ObjectId]struct{}
		m activeTaskStats
	}
}

func NewMergeExecutor(rt *dbutils.Runtime, sched CNMergeScheduler) *MergeExecutor {
	return &MergeExecutor{
		rt:      rt,
		cnSched: sched,
	}
}

func (e *MergeExecutor) setSpareMem(total uint64) {
	containerMLimit, err := memlimit.FromCgroup()
	tenth := int(float64(total) * 0.1)
	limitdiff := 0
	if containerMLimit > 0 {
		limitdiff = int(total - containerMLimit)
	}
	if limitdiff > tenth {
		e.memSpare = limitdiff
	} else {
		e.memSpare = tenth
	}

	availTotal := total - uint64(e.memSpare)

	if availTotal > 200*common.Const1GBytes {
		e.transPageLimit = availTotal / 25 * 2 // 8%
	} else if availTotal > 100*common.Const1GBytes {
		e.transPageLimit = availTotal / 25 * 3 // 12%
	} else if availTotal > 40*common.Const1GBytes {
		e.transPageLimit = availTotal / 25 * 4 // 16%
	} else {
		e.transPageLimit = math.MaxUint64 // no limit
	}
	logutil.Infof("[Mergeblocks] constainer memory limit %v, host mem %v, spare mem %v, trans limit %v, err %v",
		common.HumanReadableBytes(int(containerMLimit)),
		common.HumanReadableBytes(int(total)),
		common.HumanReadableBytes(e.memSpare),
		common.HumanReadableBytes(int(e.transPageLimit)),
		err)
}

func (e *MergeExecutor) RefreshMemInfo() {
	if stats, err := mem.VirtualMemory(); err == nil {
		e.memAvail = int(stats.Available)
		if e.memSpare == 0 {
			e.setSpareMem(stats.Total)
		}
	}
	if percents, err := cpu.Percent(0, false); err == nil {
		e.cpuPercent = percents[0]
	}
	e.roundMergeRows = 0
}

func (e *MergeExecutor) PrintStats() {
	cnt := atomic.LoadInt32(&e.activeMergeBlkCount)
	if cnt == 0 && e.MemAvailBytes() > 512*common.Const1MBytes {
		return
	}

	logutil.Infof(
		"Mergeblocks avail mem: %v(%v reserved), active mergeing size: %v, active merging blk cnt: %d",
		common.HumanReadableBytes(e.memAvail),
		common.HumanReadableBytes(e.memSpare),
		common.HumanReadableBytes(int(atomic.LoadInt64(&e.activeEstimateBytes))), cnt,
	)
}

func (e *MergeExecutor) AddActiveTask(taskId uint64, blkn, esize int) {
	atomic.AddInt64(&e.activeEstimateBytes, int64(esize))
	atomic.AddInt32(&e.activeMergeBlkCount, int32(blkn))
	e.taskConsume.Lock()
	if e.taskConsume.m == nil {
		e.taskConsume.m = make(activeTaskStats)
	}
	e.taskConsume.m[taskId] = struct {
		blk      int
		estBytes int
	}{blkn, esize}
	e.taskConsume.Unlock()
}

func (e *MergeExecutor) OnExecDone(v any) {
	task := v.(tasks.MScopedTask)

	e.taskConsume.Lock()
	stat := e.taskConsume.m[task.ID()]
	delete(e.taskConsume.m, task.ID())
	e.taskConsume.Unlock()

	atomic.AddInt32(&e.activeMergeBlkCount, -int32(stat.blk))
	atomic.AddInt64(&e.activeEstimateBytes, -int64(stat.estBytes))
}

func (e *MergeExecutor) ExecuteFor(entry *catalog.TableEntry, mobjs []*catalog.ObjectEntry, kind TaskHostKind) {
	if e.roundMergeRows*36 /*28 * 1.3 */ > e.transPageLimit/8 {
		return
	}
	e.tableName = fmt.Sprintf("%v-%v", entry.ID, entry.GetLastestSchema().Name)

	if ActiveCNObj.CheckOverlapOnCNActive(mobjs) {
		return
	}

	osize, esize, _ := estimateMergeConsume(mobjs)
	blkCnt := 0
	for _, obj := range mobjs {
		blkCnt += obj.BlockCnt()
	}
	if kind == TaskHostCN {
		stats := make([][]byte, 0, len(mobjs))
		cids := make([]common.ID, 0, len(mobjs))
		for _, obj := range mobjs {
			stat := obj.GetObjectStats()
			stats = append(stats, stat.Clone().Marshal())
			cids = append(cids, *obj.AsCommonID())
		}
		if e.rt.Scheduler.CheckAsyncScopes(cids) != nil {
			return
		}
		schema := entry.GetLastestSchema()
		cntask := &api.MergeTaskEntry{
			AccountId:         schema.AcInfo.TenantID,
			UserId:            schema.AcInfo.UserID,
			RoleId:            schema.AcInfo.RoleID,
			TblId:             entry.ID,
			DbId:              entry.GetDB().GetID(),
			TableName:         entry.GetLastestSchema().Name,
			DbName:            entry.GetDB().GetName(),
			ToMergeObjs:       stats,
			EstimatedMemUsage: uint64(esize),
		}
		if err := e.cnSched.SendMergeTask(context.TODO(), cntask); err == nil {
			ActiveCNObj.AddActiveCNObj(mobjs)
			logMergeTask(e.tableName, math.MaxUint64, mobjs, blkCnt, osize, esize)
		} else {
			logutil.Warnf("mergeblocks send to cn error: %v", err)
			return
		}
	} else {
		scopes := make([]common.ID, len(mobjs))
		for i, obj := range mobjs {
			scopes[i] = *obj.AsCommonID()
		}

		factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
			task, err := jobs.NewMergeObjectsTask(ctx, txn, mobjs, e.rt, common.DefaultMaxOsizeObjMB*common.Const1MBytes)
			return task, err
		}
		task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTaskWithObserver(nil, tasks.DataCompactionTask, scopes, factory, e)
		if err != nil {
			if err != tasks.ErrScheduleScopeConflict {
				logutil.Infof("[Mergeblocks] Schedule error info=%v", err)
			}
			return
		}
		e.AddActiveTask(task.ID(), blkCnt, esize)
		for _, obj := range mobjs {
			e.roundMergeRows += uint64(obj.GetRemainingRows())
		}
		logMergeTask(e.tableName, task.ID(), mobjs, blkCnt, osize, esize)
	}

	entry.Stats.AddMerge(osize, len(mobjs), blkCnt)
}

func (e *MergeExecutor) MemAvailBytes() int {
	merging := int(atomic.LoadInt64(&e.activeEstimateBytes))
	avail := e.memAvail - e.memSpare - merging
	if avail < 0 {
		avail = 0
	}
	return avail
}

func (e *MergeExecutor) TransferPageSizeLimit() uint64 {
	return e.transPageLimit
}

func (e *MergeExecutor) CPUPercent() int64 {
	return int64(e.cpuPercent)
}

func logMergeTask(name string, taskId uint64, merges []*catalog.ObjectEntry, blkn, osize, esize int) {
	rows := 0
	infoBuf := &bytes.Buffer{}
	for _, obj := range merges {
		r := obj.GetRemainingRows()
		rows += r
		infoBuf.WriteString(fmt.Sprintf(" %d(%s)", r, common.ShortObjId(obj.ID)))
	}
	platform := fmt.Sprintf("t%d", taskId)
	if taskId == math.MaxUint64 {
		platform = "CN"
		v2.TaskCNMergeScheduledByCounter.Inc()
		v2.TaskCNMergedSizeCounter.Add(float64(osize))
	} else {
		v2.TaskDNMergeScheduledByCounter.Inc()
		v2.TaskDNMergedSizeCounter.Add(float64(osize))
	}
	logutil.Infof(
		"[Mergeblocks] Scheduled %v [%v|on%d,bn%d|%s,%s], merged(%v): %s", name,
		platform, len(merges), blkn,
		common.HumanReadableBytes(osize), common.HumanReadableBytes(esize),
		rows,
		infoBuf.String(),
	)
}
