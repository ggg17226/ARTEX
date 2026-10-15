package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// QueryRower 抽象 sql.DB 与 sql.Tx 的单行查询能力
type QueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// DBTX 抽象标准库 SQL 执行与查询接口
type DBTX interface {
	QueryRower
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// 关系与检查约束标准错误
var (
	ErrForeignKeyViolation      = errors.New("relation_validator: referenced parent entity not found or deleted")
	ErrCheckConstraintViolation = errors.New("relation_validator: check constraint violation")
)

// ValidationError 详细记录入口校验违规事实
type ValidationError struct {
	Table  string
	Column string
	Value  any
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed on %s.%s (%v): %s", e.Table, e.Column, e.Value, e.Reason)
}

func (e *ValidationError) Unwrap() error {
	return ErrCheckConstraintViolation
}

// ValidateTaskExists 验证指定 Task 存在且未被逻辑删除
func ValidateTaskExists(ctx context.Context, q QueryRower, taskID uint64) error {
	if taskID == 0 {
		return &ValidationError{Table: "tasks", Column: "id", Value: taskID, Reason: "task_id must be positive"}
	}
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM tasks WHERE id = ? AND deleted_at IS NULL LIMIT 1`, taskID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: tasks id=%d", ErrForeignKeyViolation, taskID)
		}
		return err
	}
	return nil
}

// ValidateCompanyExists 验证指定 Company 存在
func ValidateCompanyExists(ctx context.Context, q QueryRower, companyID uint64) error {
	if companyID == 0 {
		return &ValidationError{Table: "companies", Column: "id", Value: companyID, Reason: "company_id must be positive"}
	}
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM companies WHERE id = ? LIMIT 1`, companyID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: companies id=%d", ErrForeignKeyViolation, companyID)
		}
		return err
	}
	return nil
}

// ValidateLLMProfileExists 验证指定 LLM Profile 存在
func ValidateLLMProfileExists(ctx context.Context, q QueryRower, profileID uint64) error {
	if profileID == 0 {
		return &ValidationError{Table: "llm_profiles", Column: "id", Value: profileID, Reason: "profile_id must be positive"}
	}
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM llm_profiles WHERE id = ? LIMIT 1`, profileID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: llm_profiles id=%d", ErrForeignKeyViolation, profileID)
		}
		return err
	}
	return nil
}

// ValidateCategoryExists 验证指定 Task Category 存在
func ValidateCategoryExists(ctx context.Context, q QueryRower, categoryID uint64) error {
	if categoryID == 0 {
		return &ValidationError{Table: "task_categories", Column: "id", Value: categoryID, Reason: "category_id must be positive"}
	}
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM task_categories WHERE id = ? LIMIT 1`, categoryID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: task_categories id=%d", ErrForeignKeyViolation, categoryID)
		}
		return err
	}
	return nil
}

// ValidateExplorationExists 验证指定 Exploration 存在
func ValidateExplorationExists(ctx context.Context, q QueryRower, explorationID uint64) error {
	if explorationID == 0 {
		return &ValidationError{Table: "explorations", Column: "id", Value: explorationID, Reason: "exploration_id must be positive"}
	}
	var exists int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM explorations WHERE id = ? LIMIT 1`, explorationID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: explorations id=%d", ErrForeignKeyViolation, explorationID)
		}
		return err
	}
	return nil
}

// ValidateExplorationNodeBelongs 验证节点存在且属于指定 exploration
func ValidateExplorationNodeBelongs(ctx context.Context, q QueryRower, nodeID, expectedExplorationID uint64) error {
	if nodeID == 0 {
		return &ValidationError{Table: "exploration_nodes", Column: "id", Value: nodeID, Reason: "node_id must be positive"}
	}
	var actualExpID uint64
	err := q.QueryRowContext(ctx, `SELECT exploration_id FROM exploration_nodes WHERE id = ? LIMIT 1`, nodeID).Scan(&actualExpID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: exploration_nodes id=%d", ErrForeignKeyViolation, nodeID)
		}
		return err
	}
	if actualExpID != expectedExplorationID {
		return &ValidationError{
			Table:  "exploration_nodes",
			Column: "exploration_id",
			Value:  actualExpID,
			Reason: fmt.Sprintf("node id=%d belongs to exploration %d, not expected %d", nodeID, actualExpID, expectedExplorationID),
		}
	}
	return nil
}

// ValidateExplorationEdge 验证边的起点与终点存在、属于同一 exploration，且 src != dst
func ValidateExplorationEdge(ctx context.Context, q QueryRower, explorationID, srcID, dstID uint64, rel string) error {
	if srcID == dstID {
		return &ValidationError{Table: "exploration_edges", Column: "dst_id", Value: dstID, Reason: "src_id and dst_id must be different"}
	}
	validRels := map[string]bool{
		"spawns": true, "derived_from": true, "yields": true, "proves": true, "covers": true,
	}
	if !validRels[rel] {
		return &ValidationError{Table: "exploration_edges", Column: "rel", Value: rel, Reason: "invalid exploration_edge rel"}
	}
	if err := ValidateExplorationNodeBelongs(ctx, q, srcID, explorationID); err != nil {
		return fmt.Errorf("src node invalid: %w", err)
	}
	if err := ValidateExplorationNodeBelongs(ctx, q, dstID, explorationID); err != nil {
		return fmt.Errorf("dst node invalid: %w", err)
	}
	return nil
}

// ValidateAssetFields 验证资产表的 type, company_source, port, service_type
func ValidateAssetFields(assetType, companySource string, port *int, serviceType *string) error {
	validTypes := map[string]bool{
		"root_domain": true, "ip": true, "subdomain": true, "app": true, "service": true, "endpoint": true,
	}
	if !validTypes[assetType] {
		return &ValidationError{Table: "assets", Column: "type", Value: assetType, Reason: "invalid asset type"}
	}
	validSources := map[string]bool{
		"explicit": true, "scope": true,
	}
	if !validSources[companySource] {
		return &ValidationError{Table: "assets", Column: "company_source", Value: companySource, Reason: "invalid company_source"}
	}
	if port != nil {
		if *port < 1 || *port > 65535 {
			return &ValidationError{Table: "assets", Column: "port", Value: *port, Reason: "port must be between 1 and 65535"}
		}
	}
	if serviceType != nil {
		if *serviceType != "http" && *serviceType != "other" {
			return &ValidationError{Table: "assets", Column: "service_type", Value: *serviceType, Reason: "service_type must be http or other"}
		}
	}
	return nil
}

// ValidateCompanyScopePayload 验证 company_scope 的 3 分支互斥规则
func ValidateCompanyScopePayload(kind string, domain, netVal, val *string) error {
	validKinds := map[string]bool{
		"domain": true, "ip": true, "cidr": true, "icp": true, "keyword": true,
	}
	if !validKinds[kind] {
		return &ValidationError{Table: "company_scope", Column: "kind", Value: kind, Reason: "invalid company_scope kind"}
	}
	switch kind {
	case "domain":
		if domain == nil || *domain == "" || netVal != nil || val != nil {
			return fmt.Errorf("%w: kind=domain requires domain NOT NULL and net/value NULL", ErrCheckConstraintViolation)
		}
	case "ip", "cidr":
		if netVal == nil || *netVal == "" || domain != nil || val != nil {
			return fmt.Errorf("%w: kind=ip/cidr requires net NOT NULL and domain/value NULL", ErrCheckConstraintViolation)
		}
	case "icp", "keyword":
		if val == nil || *val == "" || domain != nil || netVal != nil {
			return fmt.Errorf("%w: kind=icp/keyword requires value NOT NULL and domain/net NULL", ErrCheckConstraintViolation)
		}
	}
	return nil
}

// ValidateSideQuestionSessionScope 验证 side_question_sessions 会话作用域的 XOR 规则
func ValidateSideQuestionSessionScope(conversationID *uint64, taskID, explorationID, intentID *uint64) error {
	if conversationID != nil && *conversationID > 0 {
		if (taskID != nil && *taskID > 0) || (explorationID != nil && *explorationID > 0) || (intentID != nil && *intentID > 0) {
			return fmt.Errorf("%w: side_question_sessions with conversation_id must have NULL task/exploration/intent", ErrCheckConstraintViolation)
		}
		return nil
	}
	// conversationID 为空时，taskID 与 explorationID 必须非空
	if taskID == nil || *taskID == 0 || explorationID == nil || *explorationID == 0 {
		return fmt.Errorf("%w: side_question_sessions without conversation_id must have non-NULL task and exploration", ErrCheckConstraintViolation)
	}
	return nil
}

// ValidateTaskConstraints 验证 task_constraints 规则类型与禁止自环
func ValidateTaskConstraints(kind string, taskID, sourceTaskID uint64) error {
	if kind != "allow" && kind != "deny" {
		return &ValidationError{Table: "task_constraints", Column: "kind", Value: kind, Reason: "kind must be allow or deny"}
	}
	if taskID == sourceTaskID {
		return fmt.Errorf("%w: task_constraints self-referential task_id == source_task_id", ErrCheckConstraintViolation)
	}
	return nil
}
