//go:build !windows

package cmd

import (
	"testing"
)

func TestParseWrapArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantName   string
		wantClaude []string
		wantHelp   bool
	}{
		{
			name:       "no args",
			args:       []string{},
			wantName:   "",
			wantClaude: nil,
			wantHelp:   false,
		},
		{
			name:       "only claude args (backwards compat)",
			args:       []string{"-p", "fix the bug"},
			wantName:   "",
			wantClaude: []string{"-p", "fix the bug"},
			wantHelp:   false,
		},
		{
			name:       "double dash separator with claude args",
			args:       []string{"--", "-p", "fix the bug"},
			wantName:   "",
			wantClaude: []string{"-p", "fix the bug"},
			wantHelp:   false,
		},
		{
			name:       "name flag with double dash",
			args:       []string{"--name", "backend", "--", "--resume"},
			wantName:   "backend",
			wantClaude: []string{"--resume"},
			wantHelp:   false,
		},
		{
			name:       "short name flag with double dash",
			args:       []string{"-n", "frontend", "--", "-p", "hello"},
			wantName:   "frontend",
			wantClaude: []string{"-p", "hello"},
			wantHelp:   false,
		},
		{
			name:       "name flag without double dash",
			args:       []string{"--name", "backend"},
			wantName:   "backend",
			wantClaude: nil,
			wantHelp:   false,
		},
		{
			name:       "name flag mixed with claude args no separator",
			args:       []string{"--name", "backend", "--resume"},
			wantName:   "backend",
			wantClaude: []string{"--resume"},
			wantHelp:   false,
		},
		{
			name:       "help flag",
			args:       []string{"--help"},
			wantName:   "",
			wantClaude: nil,
			wantHelp:   true,
		},
		{
			name:       "help short flag",
			args:       []string{"-h"},
			wantName:   "",
			wantClaude: nil,
			wantHelp:   true,
		},
		{
			name:       "double dash only",
			args:       []string{"--"},
			wantName:   "",
			wantClaude: nil,
			wantHelp:   false,
		},
		{
			name:       "name and help before double dash",
			args:       []string{"--name", "test", "-h", "--"},
			wantName:   "test",
			wantClaude: nil,
			wantHelp:   true,
		},
		{
			name:       "resume session with double dash",
			args:       []string{"--", "--resume", "1335765446"},
			wantName:   "",
			wantClaude: []string{"--resume", "1335765446"},
			wantHelp:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotClaude, gotHelp := parseWrapArgs(tt.args)

			if gotName != tt.wantName {
				t.Errorf("name = %q, want %q", gotName, tt.wantName)
			}

			if gotHelp != tt.wantHelp {
				t.Errorf("help = %v, want %v", gotHelp, tt.wantHelp)
			}

			if len(gotClaude) != len(tt.wantClaude) {
				t.Errorf("claudeArgs = %v, want %v", gotClaude, tt.wantClaude)
				return
			}
			for i := range gotClaude {
				if gotClaude[i] != tt.wantClaude[i] {
					t.Errorf("claudeArgs[%d] = %q, want %q", i, gotClaude[i], tt.wantClaude[i])
				}
			}
		})
	}
}
