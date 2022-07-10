// Copyright 2021 - 2022 Matrix Origin
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

package hakeeper

import (
	"encoding/binary"
	"encoding/gob"
	"io"
	"time"

	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

var (
	plog = logger.GetLogger("hakeeper")
)

var (
	binaryEnc = binary.BigEndian
)

const (
	// TickDuration defines the frequency of ticks.
	TickDuration = time.Second
	// CheckDuration defines how often HAKeeper checks the health state of the cluster
	CheckDuration = time.Second
	// DefaultHAKeeperShardID is the shard ID assigned to the special HAKeeper
	// shard.
	DefaultHAKeeperShardID uint64 = 0
	headerSize                    = pb.HeaderSize
)

type StateQuery struct{}
type logShardIDQuery struct{ name string }
type logShardIDQueryResult struct{ id uint64 }
type ScheduleCommandQuery struct{ UUID string }

type stateMachine struct {
	replicaID uint64
	state     pb.HAKeeperRSMState
}

func parseCmdTag(cmd []byte) pb.HAKeeperUpdateType {
	return pb.HAKeeperUpdateType(binaryEnc.Uint32(cmd))
}

func GetInitialClusterRequestCmd(numOfLogShards uint64,
	numOfDNShards uint64, numOfLogReplicas uint64) []byte {
	req := pb.InitialClusterRequest{
		NumOfLogShards:   numOfLogShards,
		NumOfDNShards:    numOfDNShards,
		NumOfLogReplicas: numOfLogReplicas,
	}
	payload, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	cmd := make([]byte, headerSize+len(payload))
	binaryEnc.PutUint32(cmd, uint32(pb.InitialClusterUpdate))
	copy(cmd[headerSize:], payload)
	return cmd
}

func isInitialClusterRequestCmd(cmd []byte) bool {
	return parseCmdTag(cmd) == pb.InitialClusterUpdate
}

func parseInitialClusterRequestCmd(cmd []byte) pb.InitialClusterRequest {
	if parseCmdTag(cmd) != pb.InitialClusterUpdate {
		panic("not a initial cluster update")
	}
	payload := cmd[headerSize:]
	var result pb.InitialClusterRequest
	if err := result.Unmarshal(payload); err != nil {
		panic(err)
	}
	return result
}

func GetUpdateCommandsCmd(term uint64, cmds []pb.ScheduleCommand) []byte {
	b := pb.CommandBatch{
		Term:     term,
		Commands: cmds,
	}
	data := make([]byte, headerSize+b.Size())
	binaryEnc.PutUint32(data, uint32(pb.ScheduleCommandUpdate))
	if _, err := b.MarshalTo(data[headerSize:]); err != nil {
		panic(err)
	}
	return data
}

func isUpdateCommandsCmd(cmd []byte) bool {
	return parseCmdTag(cmd) == pb.ScheduleCommandUpdate
}

func GetGetIDCmd(count uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.GetIDUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], count)
	return cmd
}

func isCNHeartbeatCmd(cmd []byte) bool {
	return isHeartbeatCmd(cmd, pb.CNHeartbeatUpdate)
}

func isDNHeartbeatCmd(cmd []byte) bool {
	return isHeartbeatCmd(cmd, pb.DNHeartbeatUpdate)
}

func isLogHeartbeatCmd(cmd []byte) bool {
	return isHeartbeatCmd(cmd, pb.LogHeartbeatUpdate)
}

func isHeartbeatCmd(cmd []byte, tag pb.HAKeeperUpdateType) bool {
	if len(cmd) <= headerSize {
		return false
	}
	return parseCmdTag(cmd) == tag
}

func parseHeartbeatCmd(cmd []byte) []byte {
	return cmd[headerSize:]
}

func isTickCmd(cmd []byte) bool {
	return len(cmd) == headerSize && parseCmdTag(cmd) == pb.TickUpdate
}

func isGetIDCmd(cmd []byte) bool {
	return len(cmd) == headerSize+8 && parseCmdTag(cmd) == pb.GetIDUpdate
}

func parseGetIDCmd(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func isSetStateCmd(cmd []byte) bool {
	return len(cmd) == headerSize+4 && parseCmdTag(cmd) == pb.SetStateUpdate
}

func parseSetStateCmd(cmd []byte) pb.HAKeeperState {
	return pb.HAKeeperState(binaryEnc.Uint32(cmd[headerSize:]))
}

func GetSetStateCmd(state pb.HAKeeperState) []byte {
	cmd := make([]byte, headerSize+4)
	binaryEnc.PutUint32(cmd, uint32(pb.SetStateUpdate))
	binaryEnc.PutUint32(cmd[headerSize:], uint32(state))
	return cmd
}

func GetTickCmd() []byte {
	cmd := make([]byte, headerSize)
	binaryEnc.PutUint32(cmd, uint32(pb.TickUpdate))
	return cmd
}

func GetLogStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.LogHeartbeatUpdate)
}

func GetCNStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.CNHeartbeatUpdate)
}

func GetDNStoreHeartbeatCmd(data []byte) []byte {
	return getHeartbeatCmd(data, pb.DNHeartbeatUpdate)
}

func getHeartbeatCmd(data []byte, tag pb.HAKeeperUpdateType) []byte {
	cmd := make([]byte, headerSize+len(data))
	binaryEnc.PutUint32(cmd, uint32(tag))
	copy(cmd[headerSize:], data)
	return cmd
}

func NewStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	if shardID != DefaultHAKeeperShardID {
		panic(moerr.NewError(moerr.INVALID_INPUT, "invalid HAKeeper shard ID"))
	}
	return &stateMachine{
		replicaID: replicaID,
		state:     pb.NewRSMState(),
	}
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) assignID() uint64 {
	s.state.NextID++
	return s.state.NextID
}

func (s *stateMachine) handleUpdateCommandsCmd(cmd []byte) sm.Result {
	data := cmd[headerSize:]
	var b pb.CommandBatch
	if err := b.Unmarshal(data); err != nil {
		panic(err)
	}
	plog.Infof("incoming term: %d, rsm term: %d", b.Term, s.state.Term)
	if s.state.Term > b.Term {
		return sm.Result{}
	}

	for _, c := range b.Commands {
		if c.Bootstrapping {
			if s.state.State != pb.HAKeeperBootstrapping {
				return sm.Result{}
			}
		}
	}

	s.state.Term = b.Term
	s.state.ScheduleCommands = make(map[string]pb.CommandBatch)
	for _, c := range b.Commands {
		if c.Bootstrapping {
			s.handleSetStateCmd(GetSetStateCmd(pb.HAKeeperBootstrapCommandsReceived))
		}
		l, ok := s.state.ScheduleCommands[c.UUID]
		if !ok {
			l = pb.CommandBatch{
				Commands: make([]pb.ScheduleCommand, 0),
			}
		}
		l.Commands = append(l.Commands, c)
		s.state.ScheduleCommands[c.UUID] = l
	}

	return sm.Result{}
}

func (s *stateMachine) handleCNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.CNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.CNState.Update(hb, s.state.Tick)
	return sm.Result{}
}

func (s *stateMachine) handleDNHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.DNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.DNState.Update(hb, s.state.Tick)
	return sm.Result{}
}

func (s *stateMachine) handleLogHeartbeat(cmd []byte) sm.Result {
	data := parseHeartbeatCmd(cmd)
	var hb pb.LogStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.state.LogState.Update(hb, s.state.Tick)
	return sm.Result{}
}

func (s *stateMachine) handleTick(cmd []byte) sm.Result {
	s.state.Tick++
	return sm.Result{}
}

func (s *stateMachine) handleGetIDCmd(cmd []byte) sm.Result {
	count := parseGetIDCmd(cmd)
	s.state.NextID++
	v := s.state.NextID
	s.state.NextID += (count - 1)
	return sm.Result{Value: v}
}

func (s *stateMachine) handleSetStateCmd(cmd []byte) sm.Result {
	re := func() sm.Result {
		data := make([]byte, 4)
		binaryEnc.PutUint32(data, uint32(s.state.State))
		return sm.Result{Data: data}
	}
	state := parseSetStateCmd(cmd)
	switch s.state.State {
	case pb.HAKeeperCreated:
		return re()
	case pb.HAKeeperBootstrapping:
		if state == pb.HAKeeperBootstrapCommandsReceived {
			s.state.State = state
			return sm.Result{}
		}
		return re()
	case pb.HAKeeperBootstrapCommandsReceived:
		if state == pb.HAKeeperBootstrapFailed || state == pb.HAKeeperRunning {
			s.state.State = state
			return sm.Result{}
		}
		return re()
	case pb.HAKeeperBootstrapFailed:
		return re()
	case pb.HAKeeperRunning:
		return re()
	default:
		panic("unknown HAKeeper state")
	}
}

func (s *stateMachine) handleInitialClusterRequestCmd(cmd []byte) sm.Result {
	result := sm.Result{Value: uint64(s.state.State)}
	if s.state.State != pb.HAKeeperCreated {
		return result
	}
	req := parseInitialClusterRequestCmd(cmd)
	if req.NumOfLogShards != req.NumOfDNShards {
		panic("DN:Log 1:1 mode is the only supported mode")
	}

	// FIXME: NextID should be initialized to 1, as 0 is already statically
	// assigned to HAKeeper itself
	s.state.NextID++
	dnShards := make([]metadata.DNShardRecord, 0)
	logShards := make([]metadata.LogShardRecord, 0)
	for i := uint64(0); i < req.NumOfLogShards; i++ {
		rec := metadata.LogShardRecord{
			ShardID:          s.state.NextID,
			NumberOfReplicas: req.NumOfLogReplicas,
		}
		s.state.NextID++
		logShards = append(logShards, rec)

		drec := metadata.DNShardRecord{
			ShardID:    s.state.NextID,
			LogShardID: rec.ShardID,
		}
		s.state.NextID++
		dnShards = append(dnShards, drec)
	}
	s.state.ClusterInfo = pb.ClusterInfo{
		DNShards:  dnShards,
		LogShards: logShards,
	}
	plog.Infof("HAKeeper set to the BOOTSTRAPPING state")
	s.state.State = pb.HAKeeperBootstrapping
	return result
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	// TODO: we need to make sure InitialClusterRequestCmd is the
	// first user cmd added to the Raft log
	cmd := e.Cmd
	if isDNHeartbeatCmd(cmd) {
		return s.handleDNHeartbeat(cmd), nil
	} else if isCNHeartbeatCmd(cmd) {
		return s.handleCNHeartbeat(cmd), nil
	} else if isLogHeartbeatCmd(cmd) {
		return s.handleLogHeartbeat(cmd), nil
	} else if isTickCmd(cmd) {
		return s.handleTick(cmd), nil
	} else if isGetIDCmd(cmd) {
		return s.handleGetIDCmd(cmd), nil
	} else if isUpdateCommandsCmd(cmd) {
		return s.handleUpdateCommandsCmd(cmd), nil
	} else if isSetStateCmd(cmd) {
		return s.handleSetStateCmd(cmd), nil
	} else if isInitialClusterRequestCmd(cmd) {
		return s.handleInitialClusterRequestCmd(cmd), nil
	}
	panic(moerr.NewError(moerr.INVALID_INPUT, "unexpected haKeeper cmd"))
}

func (s *stateMachine) handleStateQuery() interface{} {
	// FIXME: pretty sure we need to deepcopy here
	return &pb.CheckerState{
		Tick:        s.state.Tick,
		ClusterInfo: s.state.ClusterInfo,
		DNState:     s.state.DNState,
		LogState:    s.state.LogState,
		State:       s.state.State,
	}
}

func (s *stateMachine) handleShardIDQuery(name string) *logShardIDQueryResult {
	id, ok := s.state.LogShards[name]
	if ok {
		return &logShardIDQueryResult{id: id}
	}
	return &logShardIDQueryResult{}
}

func (s *stateMachine) handleScheduleCommandQuery(uuid string) *pb.CommandBatch {
	if batch, ok := s.state.ScheduleCommands[uuid]; ok {
		return &batch
	}
	return &pb.CommandBatch{}
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if q, ok := query.(*logShardIDQuery); ok {
		return s.handleShardIDQuery(q.name), nil
	} else if _, ok := query.(*StateQuery); ok {
		return s.handleStateQuery(), nil
	} else if q, ok := query.(*ScheduleCommandQuery); ok {
		return s.handleScheduleCommandQuery(q.UUID), nil
	}
	panic("unknown query type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	// FIXME: ready to use gogoproto to marshal the state, just need to figure
	// out how to write to the writer.
	enc := gob.NewEncoder(w)
	return enc.Encode(s.state)
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(&s.state)
}
