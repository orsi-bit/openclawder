package tty

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"unsafe"
)

// GetTTY returns the TTY path for the current process or its parent chain
func GetTTY() string {
	// Try our own fds first
	if path := getTTYFromFd(int(os.Stdin.Fd())); path != "" {
		return path
	}
	if path := getTTYFromFd(int(os.Stdout.Fd())); path != "" {
		return path
	}
	if path := getTTYFromFd(int(os.Stderr.Fd())); path != "" {
		return path
	}

	// If we don't have a TTY, try to get it from parent processes
	// This is needed because MCP servers are spawned without a TTY
	if path := getTTYFromParent(); path != "" {
		return path
	}

	return ""
}

// getTTYFromParent walks up the process tree to find a TTY
func getTTYFromParent() string {
	pid := os.Getppid()

	// Walk up to 5 levels of parent processes
	for i := 0; i < 5 && pid > 1; i++ {
		// On macOS, use ps to get the TTY of a process
		cmd := exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "tty=")
		output, err := cmd.Output()
		if err == nil {
			tty := strings.TrimSpace(string(output))
			if tty != "" && tty != "??" && tty != "-" {
				// Convert short form (ttys003) to full path (/dev/ttys003)
				if !strings.HasPrefix(tty, "/") {
					tty = "/dev/" + tty
				}
				return tty
			}
		}

		// Get parent of this pid
		cmd = exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "ppid=")
		output, err = cmd.Output()
		if err != nil {
			break
		}
		newPid := 0
		fmt.Sscanf(strings.TrimSpace(string(output)), "%d", &newPid)
		if newPid <= 1 {
			break
		}
		pid = newPid
	}

	return ""
}

func getTTYFromFd(fd int) string {
	// Check if fd is a terminal
	var termios syscall.Termios
	_, _, err := syscall.Syscall6(
		syscall.SYS_IOCTL,
		uintptr(fd),
		uintptr(syscall.TIOCGETA),
		uintptr(unsafe.Pointer(&termios)),
		0, 0, 0,
	)
	if err != 0 {
		return ""
	}

	// Get the tty name
	// On macOS/BSD, we can use /dev/fd/N to get the tty path
	// But a more reliable way is to use the ttyname syscall equivalent
	link := fmt.Sprintf("/dev/fd/%d", fd)
	path, err2 := os.Readlink(link)
	if err2 != nil {
		// Fallback: try to find which /dev/tty* this fd refers to
		// by checking if /dev/tty is available
		if isTerminal(fd) {
			return fmt.Sprintf("/dev/fd/%d", fd)
		}
		return ""
	}
	return path
}

func isTerminal(fd int) bool {
	var termios syscall.Termios
	_, _, err := syscall.Syscall6(
		syscall.SYS_IOCTL,
		uintptr(fd),
		uintptr(syscall.TIOCGETA),
		uintptr(unsafe.Pointer(&termios)),
		0, 0, 0,
	)
	return err == 0
}

// TIOCSTI is the ioctl to simulate terminal input
const TIOCSTI = 0x80017472 // macOS value

// SendToTTY sends text to a TTY device, simulating keyboard input
// Uses TIOCSTI ioctl to inject characters into the terminal input queue
func SendToTTY(ttyPath, text string) error {
	f, err := os.OpenFile(ttyPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open tty %s: %w", ttyPath, err)
	}
	defer f.Close()

	// Use TIOCSTI to inject each character as if it were typed
	for _, c := range []byte(text) {
		_, _, errno := syscall.Syscall(
			syscall.SYS_IOCTL,
			f.Fd(),
			TIOCSTI,
			uintptr(unsafe.Pointer(&c)),
		)
		if errno != 0 {
			return fmt.Errorf("TIOCSTI failed: %v", errno)
		}
	}

	return nil
}

// SendToTTYWithEnter sends text followed by Enter key
// Falls back to AppleScript on macOS if TIOCSTI fails
func SendToTTYWithEnter(ttyPath, text string) error {
	err := SendToTTY(ttyPath, text+"\n")
	if err != nil {
		// Try AppleScript fallback on macOS
		return sendViaAppleScript(ttyPath, text)
	}
	return nil
}

// sendViaAppleScript uses AppleScript to send keystrokes to a terminal
// This works around TIOCSTI restrictions on modern macOS
// Note: This targets Terminal.app and requires it to have a window open
func sendViaAppleScript(ttyPath, text string) error {
	// Escape special characters for AppleScript
	escaped := strings.ReplaceAll(text, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)

	// Extract tty number from path like /dev/ttys003
	ttyNum := ""
	if strings.HasPrefix(ttyPath, "/dev/ttys") {
		ttyNum = strings.TrimPrefix(ttyPath, "/dev/ttys")
	}

	// Try to find the right Terminal window by checking tty of each tab
	// If we can't find it specifically, fall back to the frontmost Terminal window
	script := fmt.Sprintf(`
		tell application "Terminal"
			-- Try to find window with matching tty
			set targetWindow to missing value
			set targetTab to missing value

			repeat with w in windows
				repeat with t in tabs of w
					try
						if tty of t contains "%s" then
							set targetWindow to w
							set targetTab to t
							exit repeat
						end if
					end try
				end repeat
				if targetWindow is not missing value then exit repeat
			end repeat

			if targetWindow is missing value then
				-- Fall back to front window
				if (count of windows) > 0 then
					set targetWindow to front window
				else
					return "no terminal windows"
				end if
			end if

			-- Activate and type
			activate
			set frontmost of targetWindow to true
			delay 0.1

			tell application "System Events"
				keystroke "%s"
				keystroke return
			end tell
		end tell
	`, ttyNum, escaped)

	cmd := exec.Command("osascript", "-e", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("AppleScript failed: %v, output: %s", err, output)
	}
	return nil
}

// SendToTTYBestEffort tries multiple methods to send input to a TTY
func SendToTTYBestEffort(ttyPath, text string) error {
	// Try TIOCSTI first (most direct)
	if err := SendToTTY(ttyPath, text+"\n"); err == nil {
		return nil
	}

	// Try AppleScript (macOS)
	if err := sendViaAppleScript(ttyPath, text); err == nil {
		return nil
	}

	// Try tmux if available
	if err := sendViaTmux(text); err == nil {
		return nil
	}

	return fmt.Errorf("all TTY injection methods failed for %s", ttyPath)
}

// sendViaTmux sends keystrokes via tmux if we're in a tmux session
func sendViaTmux(text string) error {
	// Check if we're in tmux
	if os.Getenv("TMUX") == "" {
		return fmt.Errorf("not in tmux session")
	}

	cmd := exec.Command("tmux", "send-keys", text, "Enter")
	return cmd.Run()
}
