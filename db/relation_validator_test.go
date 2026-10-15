package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// mockQueryRower 模拟 database/sql 的单行查询接口
type mockQueryRower struct {
	err error
}

func (m *mockQueryRower) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// 由于无法直接构造 sql.Row，我们通过测试用例中的特定断言模拟，
	// 针对纯逻辑校验则直接调用无 DB 依赖的函数。
	return nil
}

func TestValidateAssetFields_Valid(t *testing.T) {
	port := 443
	serviceType := "http"
	err := ValidateAssetFields("endpoint", "explicit", &port, &serviceType)
	if err != nil {
		t.Fatalf("expected valid asset fields, got: %v", err)
	}
}

func TestValidateAssetFields_InvalidType(t *testing.T) {
	err := ValidateAssetFields("invalid_type", "explicit", nil, nil)
	if err == nil {
		t.Fatal("expected error for invalid asset type, got nil")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
	if valErr.Column != "type" {
		t.Fatalf("expected column 'type', got %s", valErr.Column)
	}
}

func TestValidateAssetFields_InvalidPort(t *testing.T) {
	invalidPort := 70000
	err := ValidateAssetFields("ip", "scope", &invalidPort, nil)
	if err == nil {
		t.Fatal("expected error for invalid port, got nil")
	}
	var valErr *ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if valErr.Column != "port" {
		t.Fatalf("expected column 'port', got %s", valErr.Column)
	}
}

func TestValidateCompanyScopePayload_MutualExclusion(t *testing.T) {
	domain := "example.com"
	netVal := "192.168.1.0/24"
	// 规则：domain 非空时，net 和 value 必须为空
	err := ValidateCompanyScopePayload("domain", &domain, &netVal, nil)
	if err == nil {
		t.Fatal("expected mutual exclusion error, got nil")
	}
	if !errors.Is(err, ErrCheckConstraintViolation) {
		t.Fatalf("expected ErrCheckConstraintViolation, got %v", err)
	}
}

func TestValidateCompanyScopePayload_ValidDomain(t *testing.T) {
	domain := "example.com"
	err := ValidateCompanyScopePayload("domain", &domain, nil, nil)
	if err != nil {
		t.Fatalf("expected valid domain scope, got %v", err)
	}
}

func TestValidateSideQuestionSessionScope_XOR(t *testing.T) {
	convID := uint64(101)
	taskID := uint64(202)
	expID := uint64(303)

	// 非法：conversation 非 NULL 时，task/exploration 必须均为 NULL
	err := ValidateSideQuestionSessionScope(&convID, &taskID, &expID, nil)
	if err == nil {
		t.Fatal("expected XOR violation error when both conversation and task are present, got nil")
	}
	if !errors.Is(err, ErrCheckConstraintViolation) {
		t.Fatalf("expected ErrCheckConstraintViolation, got: %v", err)
	}

	// 合法 1：仅 conversation 非 NULL
	if err := ValidateSideQuestionSessionScope(&convID, nil, nil, nil); err != nil {
		t.Fatalf("expected valid conversation-only scope, got %v", err)
	}

	// 合法 2：conversation NULL 且 task/exploration 非 NULL
	if err := ValidateSideQuestionSessionScope(nil, &taskID, &expID, nil); err != nil {
		t.Fatalf("expected valid task-and-exploration scope, got %v", err)
	}

	// 非法 3：全部为 NULL
	if err := ValidateSideQuestionSessionScope(nil, nil, nil, nil); err == nil {
		t.Fatal("expected error when all scope fields are nil, got nil")
	}
}

func TestValidateTaskConstraints_SelfReferenceForbidden(t *testing.T) {
	taskID := uint64(10)
	sourceTaskID := uint64(10) // 相同 ID
	err := ValidateTaskConstraints("allow", taskID, sourceTaskID)
	if err == nil {
		t.Fatal("expected error for self-referential task constraint, got nil")
	}
	if !errors.Is(err, ErrCheckConstraintViolation) {
		t.Fatalf("expected ErrCheckConstraintViolation, got %v", err)
	}
}
