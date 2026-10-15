package server

import (
	"encoding/json"
	"math"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/artex/intercept"
)

func TestParseIDField_Exact(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{
			name:    "positive 1",
			input:   "1",
			want:    1,
			wantErr: false,
		},
		{
			name:    "max int64",
			input:   "9223372036854775807",
			want:    math.MaxInt64,
			wantErr: false,
		},
		{
			name:    "js safe max int (2^53 - 1)",
			input:   "9007199254740991",
			want:    9007199254740991,
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "non numeric abc",
			input:   "abc",
			wantErr: true,
		},
		{
			name:    "negative number",
			input:   "-1",
			wantErr: true,
		},
		{
			name:    "zero",
			input:   "0",
			wantErr: true,
		},
		{
			name:    "decimal number",
			input:   "1.5",
			wantErr: true,
		},
		{
			name:    "overflow MaxInt64 + 1",
			input:   "9223372036854775808",
			wantErr: true,
		},
		{
			name:    "huge overflow",
			input:   "99999999999999999999",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseIDField(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseIDField(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("parseIDField(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseIDFieldLenient(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantIsNil bool
		wantVal   int64
	}{
		{
			name:      "empty string",
			input:     "",
			wantIsNil: true,
		},
		{
			name:      "non-numeric",
			input:     "abc",
			wantIsNil: true,
		},
		{
			name:      "negative",
			input:     "-3",
			wantIsNil: true,
		},
		{
			name:      "valid 42",
			input:     "42",
			wantIsNil: false,
			wantVal:   42,
		},
		{
			name:      "zero",
			input:     "0",
			wantIsNil: true,
		},
		{
			name:      "overflow MaxInt64 + 1",
			input:     "9223372036854775808",
			wantIsNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseIDFieldLenient(tt.input)
			if tt.wantIsNil {
				if got != nil {
					t.Fatalf("parseIDFieldLenient(%q) = %v, want nil", tt.input, *got)
				}
			} else {
				if got == nil {
					t.Fatalf("parseIDFieldLenient(%q) = nil, want %d", tt.input, tt.wantVal)
				} else if *got != tt.wantVal {
					t.Fatalf("parseIDFieldLenient(%q) = %d, want %d", tt.input, *got, tt.wantVal)
				}
			}
		})
	}
}

func TestI64Ptr(t *testing.T) {
	t.Run("nil -> nil", func(t *testing.T) {
		res := i64Ptr(nil)
		if res != nil {
			t.Fatalf("i64Ptr(nil) = %q, want nil", *res)
		}
	})

	t.Run("&int64(7) -> &\"7\"", func(t *testing.T) {
		val := int64(7)
		res := i64Ptr(&val)
		if res == nil {
			t.Fatal("i64Ptr(&7) = nil, want &\"7\"")
		}
		if *res != "7" {
			t.Fatalf("i64Ptr(&7) = %q, want \"7\"", *res)
		}
	})
}

func TestTaskDTO_EmissionStringIDs(t *testing.T) {
	catID := "100"
	dto := TaskDTO{
		ID:            "1",
		ExplorationID: "42",
		CategoryID:    &catID,
		LLMProfileIDs: []string{"2", "3"},
		CompanyIDs:    []string{"10", "20"},
	}

	bytes, err := json.Marshal(dto)
	if err != nil {
		t.Fatalf("Marshal TaskDTO failed: %v", err)
	}

	raw := string(bytes)
	if !strings.Contains(raw, `"id":"1"`) {
		t.Errorf("expected string id, got %s", raw)
	}
	if !strings.Contains(raw, `"exploration_id":"42"`) {
		t.Errorf("expected string exploration_id, got %s", raw)
	}
	if !strings.Contains(raw, `"category_id":"100"`) {
		t.Errorf("expected string category_id, got %s", raw)
	}
	if !strings.Contains(raw, `"llm_profile_ids":["2","3"]`) {
		t.Errorf("expected string array llm_profile_ids, got %s", raw)
	}
	if !strings.Contains(raw, `"company_ids":["10","20"]`) {
		t.Errorf("expected string array company_ids, got %s", raw)
	}

	// nil pointers should be omitted
	nilDTO := TaskDTO{
		ID:            "2",
		ExplorationID: "43",
	}
	bytesNil, err := json.Marshal(nilDTO)
	if err != nil {
		t.Fatalf("Marshal nilDTO failed: %v", err)
	}
	rawNil := string(bytesNil)
	if strings.Contains(rawNil, `"category_id"`) {
		t.Errorf("expected category_id omitted, got %s", rawNil)
	}
	if strings.Contains(rawNil, `"llm_profile_id"`) {
		t.Errorf("expected llm_profile_id omitted, got %s", rawNil)
	}
}

func TestParseCategoryIDField(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantNil    bool
		wantID     int64
		wantOK     bool
		wantStatus int
	}{
		{
			name:       "empty json raw",
			raw:        "",
			wantOK:     false,
			wantStatus: 400,
		},
		{
			name:       "null literal",
			raw:        "null",
			wantNil:    true,
			wantOK:     true,
			wantStatus: 200,
		},
		{
			name:       "string valid number",
			raw:        `"123"`,
			wantNil:    false,
			wantID:     123,
			wantOK:     true,
			wantStatus: 200,
		},
		{
			name:       "string empty",
			raw:        `""`,
			wantOK:     false,
			wantStatus: 400,
		},
		{
			name:       "string invalid abc",
			raw:        `"abc"`,
			wantOK:     false,
			wantStatus: 400,
		},
		{
			name:       "string negative",
			raw:        `"-5"`,
			wantOK:     false,
			wantStatus: 400,
		},
		{
			name:       "number valid int",
			raw:        `456`,
			wantNil:    false,
			wantID:     456,
			wantOK:     true,
			wantStatus: 200,
		},
		{
			name:       "number zero",
			raw:        `0`,
			wantOK:     false,
			wantStatus: 400,
		},
		{
			name:       "number negative",
			raw:        `-10`,
			wantOK:     false,
			wantStatus: 400,
		},
		{
			name:       "number overflow MaxInt64",
			raw:        `9223372036854775808`,
			wantOK:     false,
			wantStatus: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			got, ok := parseCategoryIDField(rec, json.RawMessage(tt.raw))
			if ok != tt.wantOK {
				t.Fatalf("parseCategoryIDField(%s) ok = %v, wantOK = %v, body = %s", tt.raw, ok, tt.wantOK, rec.Body.String())
			}
			if !tt.wantOK {
				if rec.Code != tt.wantStatus {
					t.Fatalf("parseCategoryIDField(%s) status = %d, want %d", tt.raw, rec.Code, tt.wantStatus)
				}
				return
			}
			if tt.wantNil {
				if got != nil {
					t.Fatalf("parseCategoryIDField(%s) = %v, want nil", tt.raw, *got)
				}
			} else {
				if got == nil {
					t.Fatalf("parseCategoryIDField(%s) = nil, want %d", tt.raw, tt.wantID)
				} else if *got != tt.wantID {
					t.Fatalf("parseCategoryIDField(%s) = %d, want %d", tt.raw, *got, tt.wantID)
				}
			}
		})
	}
}

func TestValidateTaskProfileIDs(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		s := &Server{}
		ids, err := s.validateTaskProfileIDs(nil)
		if err != nil || len(ids) != 0 {
			t.Fatalf("expected empty, got err=%v ids=%v", err, ids)
		}
	})

	t.Run("duplicate id", func(t *testing.T) {
		s := &Server{}
		_, err := s.validateTaskProfileIDs([]string{"1", "1"})
		if err == nil {
			t.Fatal("expected error for duplicate ids, got nil")
		}
	})

	t.Run("empty string id", func(t *testing.T) {
		s := &Server{}
		_, err := s.validateTaskProfileIDs([]string{""})
		if err == nil {
			t.Fatal("expected error for empty id, got nil")
		}
	})

	t.Run("non-numeric id", func(t *testing.T) {
		s := &Server{}
		_, err := s.validateTaskProfileIDs([]string{"xyz"})
		if err == nil {
			t.Fatal("expected error for non-numeric id, got nil")
		}
	})

	t.Run("non-existent profile", func(t *testing.T) {
		s := &Server{}
		_, err := s.validateTaskProfileIDs([]string{"999"})
		if err == nil {
			t.Fatal("expected error for missing profile, got nil")
		}
	})
}

func TestParseRequiredIDRaw(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr bool
	}{
		{name: "empty raw", raw: ``, wantErr: true},
		{name: "null raw", raw: `null`, wantErr: true},
		{name: "string valid 1", raw: `"1"`, want: 1, wantErr: false},
		{name: "string valid 123", raw: `"123"`, want: 123, wantErr: false},
		{name: "string valid max int64", raw: `"9223372036854775807"`, want: math.MaxInt64, wantErr: false},
		{name: "string empty", raw: `""`, wantErr: true},
		{name: "string zero", raw: `"0"`, wantErr: true},
		{name: "string negative", raw: `"-5"`, wantErr: true},
		{name: "string invalid abc", raw: `"abc"`, wantErr: true},
		{name: "string overflow", raw: `"9223372036854775808"`, wantErr: true},
		{name: "number valid 1", raw: `1`, want: 1, wantErr: false},
		{name: "number valid 123", raw: `123`, want: 123, wantErr: false},
		{name: "number valid max int64", raw: `9223372036854775807`, want: math.MaxInt64, wantErr: false},
		{name: "number zero", raw: `0`, wantErr: true},
		{name: "number negative", raw: `-1`, wantErr: true},
		{name: "number decimal", raw: `1.5`, wantErr: true},
		{name: "number overflow", raw: `9223372036854775808`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRequiredIDRaw(json.RawMessage(tt.raw))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseRequiredIDRaw(%s) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("parseRequiredIDRaw(%s) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseOptionalIDRaw(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantNil bool
		want    int64
		wantErr bool
	}{
		{name: "empty raw", raw: ``, wantNil: true, wantErr: false},
		{name: "null raw", raw: `null`, wantNil: true, wantErr: false},
		{name: "string valid 123", raw: `"123"`, want: 123, wantErr: false},
		{name: "string empty", raw: `""`, wantErr: true},
		{name: "string zero", raw: `"0"`, wantErr: true},
		{name: "string negative", raw: `"-1"`, wantErr: true},
		{name: "number valid 42", raw: `42`, want: 42, wantErr: false},
		{name: "number zero", raw: `0`, wantErr: true},
		{name: "number negative", raw: `-1`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOptionalIDRaw(json.RawMessage(tt.raw))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseOptionalIDRaw(%s) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if !tt.wantErr {
				if tt.wantNil {
					if got != nil {
						t.Fatalf("parseOptionalIDRaw(%s) = %v, want nil", tt.raw, *got)
					}
				} else {
					if got == nil {
						t.Fatalf("parseOptionalIDRaw(%s) = nil, want %d", tt.raw, tt.want)
					} else if *got != tt.want {
						t.Fatalf("parseOptionalIDRaw(%s) = %d, want %d", tt.raw, *got, tt.want)
					}
				}
			}
		})
	}
}

func TestParseProfileSaveID(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr bool
	}{
		{name: "empty raw creates profile", raw: ``, want: 0, wantErr: false},
		{name: "null creates profile", raw: `null`, want: 0, wantErr: false},
		{name: "empty string creates profile", raw: `""`, want: 0, wantErr: false},
		{name: "string 0 creates profile", raw: `"0"`, want: 0, wantErr: false},
		{name: "number 0 creates profile", raw: `0`, want: 0, wantErr: false},
		{name: "string valid 123", raw: `"123"`, want: 123, wantErr: false},
		{name: "number valid 123", raw: `123`, want: 123, wantErr: false},
		{name: "string negative", raw: `"-1"`, wantErr: true},
		{name: "number negative", raw: `-1`, wantErr: true},
		{name: "string invalid abc", raw: `"abc"`, wantErr: true},
		{name: "number overflow", raw: `9223372036854775808`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProfileSaveID(json.RawMessage(tt.raw))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseProfileSaveID(%s) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("parseProfileSaveID(%s) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseJudgeProfileID(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr bool
	}{
		{name: "empty raw sentinel", raw: ``, want: 0, wantErr: false},
		{name: "null sentinel", raw: `null`, want: 0, wantErr: false},
		{name: "string 0 sentinel", raw: `"0"`, want: 0, wantErr: false},
		{name: "number 0 sentinel", raw: `0`, want: 0, wantErr: false},
		{name: "string valid 123", raw: `"123"`, want: 123, wantErr: false},
		{name: "number valid 123", raw: `123`, want: 123, wantErr: false},
		{name: "string empty invalid", raw: `""`, wantErr: true},
		{name: "string negative", raw: `"-1"`, wantErr: true},
		{name: "number negative", raw: `-1`, wantErr: true},
		{name: "string invalid abc", raw: `"abc"`, wantErr: true},
		{name: "number overflow", raw: `9223372036854775808`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJudgeProfileID(json.RawMessage(tt.raw))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseJudgeProfileID(%s) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("parseJudgeProfileID(%s) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}

func TestNormalizeArchiveIDs(t *testing.T) {
	tests := []struct {
		name    string
		rawIDs  []string
		want    []int64
		wantErr bool
	}{
		{
			name:   "strings",
			rawIDs: []string{`"1"`, `"2"`, `"3"`},
			want:   []int64{1, 2, 3},
		},
		{
			name:   "numbers",
			rawIDs: []string{`1`, `2`, `3`},
			want:   []int64{1, 2, 3},
		},
		{
			name:   "mixed with duplicates",
			rawIDs: []string{`"1"`, `2`, `"1"`, `3`},
			want:   []int64{1, 2, 3},
		},
		{
			name:    "empty list",
			rawIDs:  []string{},
			wantErr: true,
		},
		{
			name:    "invalid id",
			rawIDs:  []string{`"0"`},
			wantErr: true,
		},
		{
			name:    "empty string id",
			rawIDs:  []string{`""`},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawMsgs := make([]json.RawMessage, len(tt.rawIDs))
			for i, r := range tt.rawIDs {
				rawMsgs[i] = json.RawMessage(r)
			}
			got, err := normalizeArchiveIDs(rawMsgs)
			if (err != nil) != tt.wantErr {
				t.Fatalf("normalizeArchiveIDs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Fatalf("normalizeArchiveIDs() got len %d, want %d", len(got), len(tt.want))
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Fatalf("normalizeArchiveIDs()[%d] = %d, want %d", i, got[i], tt.want[i])
					}
				}
			}
		})
	}
}

func TestCreateTaskReqParsing(t *testing.T) {
	t.Run("category_id string and company_ids strings", func(t *testing.T) {
		body := `{"name":"test","category_id":"10","company_ids":["20","30"],"description":"d","goal":"g"}`
		var req createTaskReq
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		catID, err := parseOptionalIDRaw(req.CategoryID)
		if err != nil || catID == nil || *catID != 10 {
			t.Fatalf("parseOptionalIDRaw(CategoryID) = %v, err: %v", catID, err)
		}
		var companyIDs []int64
		for _, raw := range req.CompanyIDs {
			id, err := parseRequiredIDRaw(raw)
			if err != nil {
				t.Fatalf("parseRequiredIDRaw company id error: %v", err)
			}
			companyIDs = append(companyIDs, id)
		}
		if len(companyIDs) != 2 || companyIDs[0] != 20 || companyIDs[1] != 30 {
			t.Fatalf("companyIDs = %v", companyIDs)
		}
	})

	t.Run("category_id null and company_ids numbers", func(t *testing.T) {
		body := `{"category_id":null,"company_ids":[100,200]}`
		var req createTaskReq
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		catID, err := parseOptionalIDRaw(req.CategoryID)
		if err != nil || catID != nil {
			t.Fatalf("parseOptionalIDRaw(CategoryID) = %v, err: %v", catID, err)
		}
		var companyIDs []int64
		for _, raw := range req.CompanyIDs {
			id, err := parseRequiredIDRaw(raw)
			if err != nil {
				t.Fatalf("parseRequiredIDRaw company id error: %v", err)
			}
			companyIDs = append(companyIDs, id)
		}
		if len(companyIDs) != 2 || companyIDs[0] != 100 || companyIDs[1] != 200 {
			t.Fatalf("companyIDs = %v", companyIDs)
		}
	})

	t.Run("category_id empty string invalid", func(t *testing.T) {
		body := `{"category_id":""}`
		var req createTaskReq
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		_, err := parseOptionalIDRaw(req.CategoryID)
		if err == nil {
			t.Fatal("expected error for empty category_id string, got nil")
		}
	})

	t.Run("company_ids invalid string element", func(t *testing.T) {
		body := `{"company_ids":["abc"]}`
		var req createTaskReq
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		if len(req.CompanyIDs) != 1 {
			t.Fatalf("expected 1 company id raw, got %d", len(req.CompanyIDs))
		}
		_, err := parseRequiredIDRaw(req.CompanyIDs[0])
		if err == nil {
			t.Fatal("expected error for company_id abc, got nil")
		}
	})
}

func TestWireTagsSerialization(t *testing.T) {
	t.Run("Conversation JSON serialization", func(t *testing.T) {
		pid := int64(456)
		conv := db.Conversation{
			ID:           123,
			AgentKey:     "agent-1",
			Title:        "Test Title",
			LLMProfileID: &pid,
		}
		data, err := json.Marshal(conv)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal map error: %v", err)
		}
		if m["id"] != "123" {
			t.Fatalf("expected id to be string '123', got %v (%T)", m["id"], m["id"])
		}
		if m["llm_profile_id"] != "456" {
			t.Fatalf("expected llm_profile_id to be string '456', got %v (%T)", m["llm_profile_id"], m["llm_profile_id"])
		}
	})

	t.Run("TaskArchive JSON serialization", func(t *testing.T) {
		catID := int64(88)
		archive := db.TaskArchive{
			ID:                 1001,
			TaskID:             2002,
			CategoryIDSnapshot: &catID,
		}
		data, err := json.Marshal(archive)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal map error: %v", err)
		}
		if m["id"] != "1001" {
			t.Fatalf("expected id to be string '1001', got %v (%T)", m["id"], m["id"])
		}
		if m["task_id"] != "2002" {
			t.Fatalf("expected task_id to be string '2002', got %v (%T)", m["task_id"], m["task_id"])
		}
		if m["category_id"] != "88" {
			t.Fatalf("expected category_id to be string '88', got %v (%T)", m["category_id"], m["category_id"])
		}
	})

	t.Run("JudgeConfig JSON serialization", func(t *testing.T) {
		cfg := intercept.JudgeConfig{
			Enabled:   true,
			ProfileID: 789,
		}
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal map error: %v", err)
		}
		if m["profile_id"] != "789" {
			t.Fatalf("expected profile_id to be string '789', got %v (%T)", m["profile_id"], m["profile_id"])
		}
	})
}
