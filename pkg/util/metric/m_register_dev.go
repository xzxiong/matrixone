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

package metric

var FSCollectors = []Collector{
	// fileservice
	FSMemCacheFactory,
}

var FSMemCacheFactory = NewCounterVec(
	CounterOpts{
		Subsystem: "fs",
		Name:      "mem_cache",
		Help:      "Counter mem cache access count",
	},
	[]string{"type"},
	false,
)

// FSMemCacheCounter accept typ as val in [hit, miss], return the counter you need.
// You can do the count like:
//
//	FSMemCacheCounter("hit").Add(number_of_hit)
//
// or FSMemCacheCounter("miss").Add(number_of_miss)
func FSMemCacheCounter(typ string) Counter {
	return FSMemCacheFactory.WithLabelValues(typ)
}

func init() {
	RegisterSubSystem(&SubSystem{"fs", "filesystem status", false})
}
