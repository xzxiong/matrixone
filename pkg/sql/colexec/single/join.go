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

package single

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "single"

func (singleJoin *SingleJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": single join ")
}

func (singleJoin *SingleJoin) OpType() vm.OpType {
	return vm.Single
}

func (singleJoin *SingleJoin) Prepare(proc *process.Process) (err error) {
	singleJoin.ctr = new(container)
	singleJoin.ctr.vecs = make([]*vector.Vector, len(singleJoin.Conditions[0]))

	singleJoin.ctr.evecs = make([]evalVector, len(singleJoin.Conditions[0]))
	for i := range singleJoin.ctr.evecs {
		singleJoin.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, singleJoin.Conditions[0][i])
		if err != nil {
			return err
		}
	}

	if singleJoin.Cond != nil {
		singleJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, singleJoin.Cond)
		if err != nil {
			return err
		}
	}

	if singleJoin.ProjectList != nil {
		err = singleJoin.PrepareProjection(proc)
	}
	return err
}

func (singleJoin *SingleJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(singleJoin.GetIdx(), singleJoin.GetParallelIdx(), singleJoin.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := singleJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			singleJoin.build(anal, proc)
			ctr.state = Probe

		case Probe:
			result, err = singleJoin.Children[0].Call(proc)
			if err != nil {
				return result, err
			}
			bat := result.Batch

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Last() {
				result.Batch = bat
				return result, nil
			}
			if bat.IsEmpty() {
				continue
			}

			anal.Input(bat, singleJoin.GetIsFirst())
			if ctr.mp == nil {
				if err := ctr.emptyProbe(bat, singleJoin, proc, &result); err != nil {
					result.Status = vm.ExecStop
					return result, err
				}
			} else {
				if err := ctr.probe(bat, singleJoin, proc, &result); err != nil {
					result.Status = vm.ExecStop
					return result, err
				}
			}

			if singleJoin.ProjectList != nil {
				var err error
				result.Batch, err = singleJoin.EvalProjection(result.Batch, proc)
				if err != nil {
					return result, err
				}
			}
			anal.Output(result.Batch, singleJoin.GetIsLast())
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}
func (singleJoin *SingleJoin) build(anal process.Analyze, proc *process.Process) {
	ctr := singleJoin.ctr
	start := time.Now()
	defer anal.WaitStop(start)
	ctr.mp = message.ReceiveJoinMap(singleJoin.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *SingleJoin, proc *process.Process, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = bat.Vecs[rp.Pos]
			bat.Vecs[rp.Pos] = nil
		} else {
			ctr.rbat.Vecs[i] = vector.NewConstNull(ap.Typs[rp.Pos], bat.RowCount(), proc.Mp())
		}
	}
	ctr.rbat.AddRowCount(bat.RowCount())
	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *SingleJoin, proc *process.Process, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel != 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(ap.Typs[rp.Pos])
		}
	}

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}

	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}

	count := bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals := itr.Find(i, n, ctr.vecs)
		for k := 0; k < n; k++ {
			if zvals[k] == 0 || vals[k] == 0 {
				for j, rp := range ap.Result {
					if rp.Rel != 0 {
						if err := ctr.rbat.Vecs[j].UnionNull(proc.Mp()); err != nil {
							return err
						}
					}
				}
				continue
			}
			if ap.HashOnPK {
				idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
				matched := false
				if ap.Cond != nil {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
						1, ctr.cfs2); err != nil {
						return err
					}
					vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
					if err != nil {
						return err
					}
					if vec.IsConstNull() || vec.GetNulls().Contains(0) {
						continue
					}
					bs := vector.MustFixedCol[bool](vec)
					if bs[0] {
						if matched {
							return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
						}
						matched = true
					}
				}
				if ap.Cond != nil && !matched {
					for j, rp := range ap.Result {
						if rp.Rel != 0 {
							if err := ctr.rbat.Vecs[j].UnionNull(proc.Mp()); err != nil {
								return err
							}
						}
					}
					continue
				}
				for j, rp := range ap.Result {
					if rp.Rel != 0 {
						if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
							return err
						}
					}
				}
			} else {
				idx := 0
				matched := false
				sels := mSels[vals[k]-1]
				if ap.Cond != nil {
					for j, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
							1, ctr.cfs1); err != nil {
							return err
						}
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2),
							1, ctr.cfs2); err != nil {
							return err
						}
						vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
						if err != nil {
							return err
						}
						if vec.IsConstNull() || vec.GetNulls().Contains(0) {
							continue
						}
						bs := vector.MustFixedCol[bool](vec)
						if bs[0] {
							if matched {
								return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
							}
							matched = true
							idx = j
						}
					}
				} else if len(sels) > 1 {
					return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
				}
				if ap.Cond != nil && !matched {
					for j, rp := range ap.Result {
						if rp.Rel != 0 {
							if err := ctr.rbat.Vecs[j].UnionNull(proc.Mp()); err != nil {
								return err
							}
						}
					}
					continue
				}
				sel := sels[idx]
				for j, rp := range ap.Result {
					if rp.Rel != 0 {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			// rbat.Vecs[i] = bat.Vecs[rp.Pos]
			// bat.Vecs[rp.Pos] = nil
			typ := *bat.Vecs[rp.Pos].GetType()
			ctr.rbat.Vecs[i] = proc.GetVector(typ)
			if err := vector.GetUnionAllFunction(typ, proc.Mp())(ctr.rbat.Vecs[i], bat.Vecs[rp.Pos]); err != nil {
				return err
			}
		}
	}
	ctr.rbat.SetRowCount(ctr.rbat.RowCount() + bat.RowCount())
	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
