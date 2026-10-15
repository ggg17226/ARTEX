package server

import (
	"encoding/json"
	"math"
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
