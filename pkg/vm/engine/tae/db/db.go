// Copyright 2021 Matrix Origin
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

package db

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	gc2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	wb "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrClosed = moerr.NewInternalErrorNoCtx("tae: closed")
)

type DB struct {
	Dir  string
	Opts *options.Options

	Catalog *catalog.Catalog

	IndexCache model.LRUCache

	TxnMgr        *txnbase.TxnManager
	TransferTable *model.HashPageTable

	LogtailMgr *logtail.Manager
	Wal        wal.Driver

	Scheduler tasks.TaskScheduler

	GCManager *gc.Manager

	BGScanner          wb.IHeartbeater
	BGCheckpointRunner checkpoint.Runner

	DiskCleaner *gc2.DiskCleaner
	Pipeline    *blockio.IoPipeline

	Fs *objectio.ObjectFS

	DBLocker io.Closer

	Closed *atomic.Value
}

func (db *DB) FlushTable(
	ctx context.Context,
	tenantID uint32,
	dbId, tableId uint64,
	ts types.TS) (err error) {
	err = db.BGCheckpointRunner.FlushTable(ctx, dbId, tableId, ts)
	return
}

func (db *DB) ForceCheckpoint(
	ctx context.Context,
	ts types.TS,
	flushDuration time.Duration) (err error) {
	// FIXME: cannot disable with a running job
	db.BGCheckpointRunner.DisableCheckpoint()
	defer db.BGCheckpointRunner.EnableCheckpoint()
	db.BGCheckpointRunner.CleanPenddingCheckpoint()
	t0 := time.Now()
	err = db.BGCheckpointRunner.ForceFlush(ts, ctx, flushDuration)
	logutil.Infof("[Force Checkpoint] flush takes %v: %v", time.Since(t0), err)
	if err != nil {
		return err
	}
	if err = db.BGCheckpointRunner.ForceIncrementalCheckpoint(ts); err != nil {
		return err
	}
	lsn := db.BGCheckpointRunner.MaxLSNInRange(ts)
	_, err = db.Wal.RangeCheckpoint(1, lsn)
	logutil.Debugf("[Force Checkpoint] takes %v", time.Since(t0))
	return err
}

func (db *DB) StartTxn(info []byte) (txnif.AsyncTxn, error) {
	return db.TxnMgr.StartTxn(info)
}

func (db *DB) StartTxnWithLatestTS(info []byte) (txnif.AsyncTxn, error) {
	return db.TxnMgr.StartTxnWithLatestTS(info)
}

func (db *DB) CommitTxn(txn txnif.AsyncTxn) (err error) {
	return txn.Commit()
}

func (db *DB) GetTxnByID(id []byte) (txn txnif.AsyncTxn, err error) {
	txn = db.TxnMgr.GetTxnByCtx(id)
	if txn == nil {
		err = moerr.NewNotFoundNoCtx()
	}
	return
}

func (db *DB) GetOrCreateTxnWithMeta(
	info []byte,
	id []byte,
	ts types.TS) (txn txnif.AsyncTxn, err error) {
	return db.TxnMgr.GetOrCreateTxnWithMeta(info, id, ts)
}

func (db *DB) RollbackTxn(txn txnif.AsyncTxn) error {
	return txn.Rollback()
}

func (db *DB) Replay(dataFactory *tables.DataFactory, maxTs types.TS) {
	// maxTs := db.Catalog.GetCheckpointed().MaxTS
	replayer := newReplayer(dataFactory, db, maxTs)
	replayer.OnTimeStamp(maxTs)
	replayer.Replay()

	err := db.TxnMgr.Init(replayer.GetMaxTS())
	if err != nil {
		panic(err)
	}
}

func (db *DB) Close() error {
	logutil.Info("gavin: [Close] start close db")
	if err := db.Closed.Load(); err != nil {
		panic(err)
	}
	db.Closed.Store(ErrClosed)
	db.GCManager.Stop()
	db.BGScanner.Stop()
	db.BGCheckpointRunner.Stop()
	db.Scheduler.Stop()
	db.TxnMgr.Stop()
	db.LogtailMgr.Stop()
	db.Wal.Close()
	db.Opts.Catalog.Close()
	db.DiskCleaner.Stop()
	db.TransferTable.Close()
	return db.DBLocker.Close()
}
