package repository

import (
	"encoding/json"
	"testing"
)

func TestRedactStatusUpdateError(t *testing.T) {
	tests := []struct {
		name    string
		context json.RawMessage
		errMsg  string
		want    string
	}{
		{
			name:    "masks rendered secret",
			context: json.RawMessage(`{"secret":{"API_KEY":"plain-value"}}`),
			errMsg:  "request failed with plain-value",
			want:    "request failed with ********",
		},
		{
			name:    "preserves ordinary error",
			context: json.RawMessage(`{"result":1}`),
			errMsg:  "request failed",
			want:    "request failed",
		},
		{
			name:    "masks when context malformed",
			context: json.RawMessage(`{"secret":`),
			errMsg:  "request failed",
			want:    "********",
		},
		{
			name:   "masks when context missing",
			errMsg: "request failed",
			want:   "********",
		},
		{
			name:    "masks when context is null",
			context: json.RawMessage(`null`),
			errMsg:  "request failed",
			want:    "********",
		},
		{
			name:    "masks when secret root malformed",
			context: json.RawMessage(`{"secret":"plain-value"}`),
			errMsg:  "request failed",
			want:    "********",
		},
		{
			name:    "masks when secret value malformed",
			context: json.RawMessage(`{"secret":{"API_KEY":{"nested":"plain-value"}}}`),
			errMsg:  "request failed",
			want:    "********",
		},
		{
			name:    "leaves empty error empty",
			context: json.RawMessage(`{}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactStatusUpdateError(tt.context, tt.errMsg); got != tt.want {
				t.Fatalf("redactStatusUpdateError() = %q, want %q", got, tt.want)
			}
		})
	}
}
