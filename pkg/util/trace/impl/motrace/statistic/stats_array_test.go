package statistic

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewStatsArray(t *testing.T) {
	type field struct {
		version      uint64
		timeConsumed uint64
		memory       uint64
		s3in         uint64
		s3out        uint64
	}
	tests := []struct {
		name       string
		field      field
		wantString []byte
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
			wantString: []byte(`[1,2,3,4,5]`),
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
			wantString: []byte(`[123,65,6,78,3494]`),
		},
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
		})
	}
}
