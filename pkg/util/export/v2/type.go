// Copyright 2024 Matrix Origin
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

package v2

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

type Item interface {
	table.RowField
	// Size returns the size of the item.
	// This is an estimate, not an exact value.
	Size() int64
	// GetName return the name of the item as identifier.
	GetName() string
	// like *table.Table help Collector do serialize this item.
}

type Content interface {
	Push(Item)
	// Full returns true if the content is full.
	Full() bool
}

type Collector interface {
	Collect(ctx context.Context, item Item) error
	Start(ctx context.Context)
	Stop(graceful bool)
}
