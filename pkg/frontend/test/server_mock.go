// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/frontend/server.go
//
// Generated by this command:
//
//	mockgen -source pkg/frontend/server.go --destination ./pkg/frontend/test/server_mock.go -package=mock_frontend
//

// Package mock_frontend is a generated GoMock package.
package mock_frontend

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	frontend "github.com/matrixorigin/matrixone/pkg/frontend"
	queryservice "github.com/matrixorigin/matrixone/pkg/queryservice"
)

// MockServer is a mock of Server interface.
type MockServer struct {
	ctrl     *gomock.Controller
	recorder *MockServerMockRecorder
}

// MockServerMockRecorder is the mock recorder for MockServer.
type MockServerMockRecorder struct {
	mock *MockServer
}

// NewMockServer creates a new mock instance.
func NewMockServer(ctrl *gomock.Controller) *MockServer {
	mock := &MockServer{ctrl: ctrl}
	mock.recorder = &MockServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockServer) EXPECT() *MockServerMockRecorder {
	return m.recorder
}

// GetRoutineManager mocks base method.
func (m *MockServer) GetRoutineManager() *frontend.RoutineManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRoutineManager")
	ret0, _ := ret[0].(*frontend.RoutineManager)
	return ret0
}

// GetRoutineManager indicates an expected call of GetRoutineManager.
func (mr *MockServerMockRecorder) GetRoutineManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRoutineManager", reflect.TypeOf((*MockServer)(nil).GetRoutineManager))
}

// Start mocks base method.
func (m *MockServer) Start() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start")
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockServerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockServer)(nil).Start))
}

// Stop mocks base method.
func (m *MockServer) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockServerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockServer)(nil).Stop))
}

// MockBaseService is a mock of BaseService interface.
type MockBaseService struct {
	ctrl     *gomock.Controller
	recorder *MockBaseServiceMockRecorder
}

// MockBaseServiceMockRecorder is the mock recorder for MockBaseService.
type MockBaseServiceMockRecorder struct {
	mock *MockBaseService
}

// NewMockBaseService creates a new mock instance.
func NewMockBaseService(ctrl *gomock.Controller) *MockBaseService {
	mock := &MockBaseService{ctrl: ctrl}
	mock.recorder = &MockBaseServiceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBaseService) EXPECT() *MockBaseServiceMockRecorder {
	return m.recorder
}

// CheckTenantUpgrade mocks base method.
func (m *MockBaseService) CheckTenantUpgrade(ctx context.Context, tenantID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckTenantUpgrade", ctx, tenantID)
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckTenantUpgrade indicates an expected call of CheckTenantUpgrade.
func (mr *MockBaseServiceMockRecorder) CheckTenantUpgrade(ctx, tenantID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckTenantUpgrade", reflect.TypeOf((*MockBaseService)(nil).CheckTenantUpgrade), ctx, tenantID)
}

// GetFinalVersion mocks base method.
func (m *MockBaseService) GetFinalVersion() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFinalVersion")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetFinalVersion indicates an expected call of GetFinalVersion.
func (mr *MockBaseServiceMockRecorder) GetFinalVersion() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFinalVersion", reflect.TypeOf((*MockBaseService)(nil).GetFinalVersion))
}

// ID mocks base method.
func (m *MockBaseService) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockBaseServiceMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockBaseService)(nil).ID))
}

// SQLAddress mocks base method.
func (m *MockBaseService) SQLAddress() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SQLAddress")
	ret0, _ := ret[0].(string)
	return ret0
}

// SQLAddress indicates an expected call of SQLAddress.
func (mr *MockBaseServiceMockRecorder) SQLAddress() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SQLAddress", reflect.TypeOf((*MockBaseService)(nil).SQLAddress))
}

// SessionMgr mocks base method.
func (m *MockBaseService) SessionMgr() *queryservice.SessionManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SessionMgr")
	ret0, _ := ret[0].(*queryservice.SessionManager)
	return ret0
}

// SessionMgr indicates an expected call of SessionMgr.
func (mr *MockBaseServiceMockRecorder) SessionMgr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SessionMgr", reflect.TypeOf((*MockBaseService)(nil).SessionMgr))
}

// UpgradeTenant mocks base method.
func (m *MockBaseService) UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpgradeTenant", ctx, tenantName, retryCount, isALLAccount)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpgradeTenant indicates an expected call of UpgradeTenant.
func (mr *MockBaseServiceMockRecorder) UpgradeTenant(ctx, tenantName, retryCount, isALLAccount any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpgradeTenant", reflect.TypeOf((*MockBaseService)(nil).UpgradeTenant), ctx, tenantName, retryCount, isALLAccount)
}
