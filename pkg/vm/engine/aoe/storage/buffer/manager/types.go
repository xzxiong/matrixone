package manager

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync"
)

type EvictNode struct {
	Handle iface.INodeHandle
	Iter   uint64
}

type IEvictHolder interface {
	sync.Locker
	Enqueue(n *EvictNode)
	Dequeue() *EvictNode
}

type BufferManager struct {
	buf.IMemoryPool
	sync.RWMutex
	Nodes       map[layout.ID]iface.INodeHandle // Manager is not responsible to Close handle
	TransientID layout.ID
	EvictHolder IEvictHolder
	Flusher     iw.IOpWorker
}
