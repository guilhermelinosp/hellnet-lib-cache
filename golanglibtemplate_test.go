package golanglibtemplate

import (
	"errors"
	"testing"
)

func TestGreet(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr error
	}{
		{name: "valid", input: "World", want: "Hello, World!"},
		{name: "empty", input: "", wantErr: ErrEmptyName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Greet(tt.input)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Greet(%q) err = %v, want %v", tt.input, err, tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("Greet(%q) unexpected err = %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("Greet(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
