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

package statistic

import (
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

func TestNewStatsArray(t *testing.T) {
	type field struct {
		version      float64
		timeConsumed float64
		memory       float64
		s3in         float64
		s3out        float64
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
		//wantAddString
		wantAddString []byte
		wantTimeVal   float64
	}{
		{
			name: "normal",
			field: field{
				version:      1,
				timeConsumed: 2,
				memory:       3,
				s3in:         4,
				s3out:        5,
			},
			wantString:    []byte(`[1,2,3,4,5]`),
			wantAddString: []byte(`[1,3,5,7,9]`),
			wantTimeVal:   float64(3),
		},
		{
			name: "random",
			field: field{
				version:      123,
				timeConsumed: 65,
				memory:       6,
				s3in:         78,
				s3out:        3494,
			},
			wantString:    []byte(`[123,65,6,78,3494]`),
			wantAddString: []byte(`[1,66,8,81,3498]`),
			wantTimeVal:   float64(66),
		},
		{
			name: "big",
			field: field{
				version:      123,
				timeConsumed: (1 << 30) * float64(24*time.Hour),
				memory:       6,
				s3in:         78,
				s3out:        3494,
			},
			wantString:    []byte(`[123,92771293593600000000000,6,78,3494]`),
			wantAddString: []byte(`[1,92771293593600000000001,8,81,3498]`),
			wantTimeVal:   float64(92771293593600000000002),
		},
	}

	getInitStatsArray := func() *StatsArray {
		return NewStatsArray().WithTimeConsumed(1).WithMemorySize(2).WithS3IOInputCount(3).WithS3IOOutputCount(4)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewStatsArray().
				WithVersion(tt.field.version).
				WithTimeConsumed(tt.field.timeConsumed).
				WithMemorySize(tt.field.memory).
				WithS3IOInputCount(tt.field.s3in).
				WithS3IOOutputCount(tt.field.s3out)
			got := s.ToJsonString()
			require.Equal(t, tt.wantString, got)
			require.Equal(t, tt.field.version, s.GetVersion())
			require.Equal(t, tt.field.timeConsumed, s.GetTimeConsumed())
			require.Equal(t, tt.field.memory, s.GetMemorySize())
			require.Equal(t, tt.field.s3in, s.GetS3IOInputCount())
			require.Equal(t, tt.field.s3out, s.GetS3IOOutputCount())

			dst := getInitStatsArray()
			gotAdd := dst.Add(s).ToJsonString()
			require.Equal(t, tt.wantTimeVal, dst[1])
			require.Equal(t, tt.wantAddString, gotAdd)
		})
	}
}

func TestNewStatsArray_big(t *testing.T) {
	v1, v2 := float64(1), float64(92771293593600000000000)
	got := v1
	got += v2
	gotStr := strconv.FormatFloat(got, 'f', 0, 64)
	require.Equal(t, float64(92771293593600000000001), got)
	require.Equal(t, "92771293593600000000000", gotStr)

	got = v2
	got += v1
	gotStr = strconv.FormatFloat(got, 'f', 0, 64)
	checkVal := float64(1024) * float64(time.Millisecond) * 1.87e-24
	t.Logf("val * 1.87e-24: %.30f", checkVal)
	t.Logf("val * 1.87e-24: %s", strconv.FormatFloat(checkVal, 'f', -1, 64))
	require.Equal(t, float64(92771293593600000000001), got)
	require.Equal(t, "92771293593600000000001", gotStr)
}
