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

// this file contains test utils. Name this file "*_test.go" to make
// compiler ignore it

package metric

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// some tests will modify global variables, and something weird
// may happen if they run concurrently.
// use this mutex to make those tests execute sequentially
var configMu *sync.Mutex = new(sync.Mutex)

func withModifiedConfig(f func()) {
	configMu.Lock()
	defer configMu.Unlock()
	f()
}

// waitWgTimeout returns an error if the WaitGroup doesn't return in timeout duration
func waitWgTimeout(wg *sync.WaitGroup, after time.Duration) error {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-time.After(time.Second):
		return errors.New("timeout")
	case <-c:
		return nil
	}
}

// waitChTimeout returns an error if:
// 1. OnRecvCheck returns an error
// 2. timeout happens before the channel running out or OnRecvCheck asking to stop
func waitChTimeout[T any](
	ch <-chan T,
	onRecvCheck func(element T, closed bool) (goOn bool, err error),
	after time.Duration,
) error {
	timeout := time.After(after)
	for {
		select {
		case <-timeout:
			return errors.New("timeout")
		case item, ok := <-ch:
			goOn, err := onRecvCheck(item, !ok)
			if err != nil {
				return err
			}
			if !ok || !goOn {
				return nil
			}
		}
	}
}

func makeDummyClock(startOffset int64) func() int64 {
	var tick int64 = startOffset - 1
	return func() int64 {
		return atomic.AddInt64(&tick, 1)
	}
}

type dummySwitch struct{}

func (dummySwitch) Start() bool                                { return true }
func (dummySwitch) Stop(graceful bool) (<-chan struct{}, bool) { return nil, false }
