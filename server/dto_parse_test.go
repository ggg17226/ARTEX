package server

import (
	"encoding/json"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
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
