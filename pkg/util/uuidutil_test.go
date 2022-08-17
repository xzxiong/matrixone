package util

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

var dummmyGetDefaultHardwareAddr = func() (net.HardwareAddr, error) {
	return net.ParseMAC("02:42:ac:11:00:02")
}

func TestSetUUIDNodeID(t *testing.T) {
	type args struct {
		nodeUuid []byte
	}
	type fields struct {
		prepare func() *gostub.Stubs
	}
	nodeUUID := uuid.Must(uuid.Parse("abddd266-238f-11ed-ba8c-d6aee46d73fa"))
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "node_uuid_with_mock",
			fields: fields{
				prepare: func() *gostub.Stubs {
					return gostub.Stub(&getDefaultHardwareAddr, dummmyGetDefaultHardwareAddr)
				},
			},
			args: args{
				nodeUuid: nodeUUID[:],
			},
			wantErr: false,
		},
		{
			name:   "node_uuid",
			fields: fields{},
			args: args{
				nodeUuid: nodeUUID[:],
			},
			wantErr: false,
		},
		{
			name:   "non-node_uuid",
			fields: fields{},
			args: args{
				nodeUuid: []byte{},
			},
			wantErr: false,
		},
		{
			name: "non-node_uuid-with_mock",
			fields: fields{
				prepare: func() *gostub.Stubs {
					return gostub.Stub(&getDefaultHardwareAddr, dummmyGetDefaultHardwareAddr)
				},
			},
			args: args{
				nodeUuid: []byte{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stubs *gostub.Stubs
			if tt.fields.prepare != nil {
				stubs = tt.fields.prepare()
			}
			if err := SetUUIDNodeID(tt.args.nodeUuid); (err != nil) != tt.wantErr {
				t.Errorf("SetUUIDNodeID() error = %v, wantErr %v", err, tt.wantErr)
			}
			localMac, _ := getDefaultHardwareAddr()
			mockMac, _ := dummmyGetDefaultHardwareAddr()
			t.Logf("local mac: %s", localMac)
			t.Logf("mock  mac: %s", mockMac)
			t.Logf("nodeUUID : %x", tt.args.nodeUuid)
			id, _ := uuid.NewUUID()
			t.Logf("uuid 1: %s", id)
			id, _ = uuid.NewUUID()
			t.Logf("uuid 2: %s", id)
			require.Equal(t, false, bytes.Equal(id[10:13], dockerMacPrefix))
			if stubs != nil {
				stubs.Reset()
			}
		})
	}
}
