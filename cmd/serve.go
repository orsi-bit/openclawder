package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maorbril/clauder/internal/mcp"
	"github.com/maorbril/clauder/internal/store"
	"github.com/maorbril/clauder/internal/tty"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the MCP server for Claude Code",
	Long:  `Starts clauder as an MCP server. This is typically invoked by Claude Code, not directly.`,
	RunE:  runServe,
}

func runServe(cmd *cobra.Command, args []string) error {
	dataDir := getDataDir()
	s, err := store.NewSQLiteStore(dataDir)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer func() { _ = s.Close() }()

	workDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get working directory: %w", err)
	}

	// Get TTY for this process
	ttyPath := tty.GetTTY()

	// Use a stable instance ID based on directory + TTY
	// This ensures messages persist across restarts, while supporting
	// multiple instances in the same directory (different terminals)
	instanceID := generateInstanceID(workDir, ttyPath)

	// Register this instance
	if err := s.RegisterInstance(instanceID, os.Getpid(), workDir, ttyPath); err != nil {
		return fmt.Errorf("failed to register instance: %w", err)
	}

	// Setup cleanup on exit
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		_ = s.UnregisterInstance(instanceID)
		cancel()
		os.Exit(0)
	}()

	// Heartbeat goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.Heartbeat(instanceID)
			}
		}
	}()

	// Run MCP server
	server := mcp.NewServer(s, instanceID, workDir)
	if err := server.Run(); err != nil {
		_ = s.UnregisterInstance(instanceID)
		return err
	}

	_ = s.UnregisterInstance(instanceID)
	return nil
}

// generateInstanceID creates a stable instance ID based on directory and TTY
// This ensures:
// - Same terminal window gets same ID across Claude Code restarts
// - Different terminals in same directory get different IDs
// - Messages persist across restarts
func generateInstanceID(directory, ttyPath string) string {
	// Combine directory and TTY for uniqueness
	// If no TTY (shouldn't happen), fall back to directory only
	input := directory
	if ttyPath != "" {
		input = directory + ":" + ttyPath
	}
	hash := sha256.Sum256([]byte(input))
	// Use first 16 bytes (32 hex chars) for a UUID-like format
	return hex.EncodeToString(hash[:16])
}

