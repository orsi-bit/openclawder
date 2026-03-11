package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/orsi-bit/openclawder/internal/telemetry"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "openclawder",
	Short: "AI agent harness for persistent memory and instance communication",
	Long: `OpenClawder is an MCP server that provides Claude Code with:
- Persistent memory (facts, decisions, context) across sessions
- Multi-instance discovery and messaging across directories
- Automatic context injection based on working directory`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stderr, "[openclawder] PersistentPreRun starting...\n")
		telemetry.SetVersion(Version)
		fmt.Fprintf(os.Stderr, "[openclawder] Initializing telemetry...\n")
		telemetry.Init()
		fmt.Fprintf(os.Stderr, "[openclawder] Telemetry initialized\n")
		// Track command usage (skip root command itself)
		if cmd.Name() != "openclawder" {
			telemetry.TrackCommand(cmd.Name())
		}
		fmt.Fprintf(os.Stderr, "[openclawder] PersistentPreRun complete\n")
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		telemetry.Close()
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(rememberCmd)
	rootCmd.AddCommand(recallCmd)
	rootCmd.AddCommand(instancesCmd)
	rootCmd.AddCommand(sendCmd)
	rootCmd.AddCommand(messagesCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(setupCmd)
	rootCmd.AddCommand(uiCmd)
	rootCmd.AddCommand(wrapCmd)
}

func getDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error getting home directory: %v\n", err)
		os.Exit(1)
	}
	newDir := home + "/.openclawder"
	oldDir := home + "/.clauder"

	// Migration: if old clauder dir exists and new openclawder dir doesn't, migrate.
	oldInfo, oldErr := os.Stat(oldDir)
	_, newErr := os.Stat(newDir)

	if oldErr == nil && oldInfo.IsDir() && os.IsNotExist(newErr) {
		// Migrate: copy old dir to new dir atomically via temp dir
		fmt.Fprintf(os.Stderr, "[openclawder] Migrating data from %s to %s ...\n", oldDir, newDir)
		tmpDir := newDir + ".tmp"
		_ = os.RemoveAll(tmpDir) // clean up any prior failed attempt
		if copyErr := copyDir(oldDir, tmpDir); copyErr != nil {
			fmt.Fprintf(os.Stderr, "[openclawder] Migration failed (will use new empty dir): %v\n", copyErr)
			_ = os.RemoveAll(tmpDir)
		} else if renameErr := os.Rename(tmpDir, newDir); renameErr != nil {
			fmt.Fprintf(os.Stderr, "[openclawder] Migration rename failed: %v\n", renameErr)
			_ = os.RemoveAll(tmpDir)
		} else {
			// Rename clauder.db → openclawder.db inside the new directory so data is preserved.
			oldDB := filepath.Join(newDir, "clauder.db")
			newDB := filepath.Join(newDir, "openclawder.db")
			if _, statErr := os.Stat(oldDB); statErr == nil {
				if _, statErr2 := os.Stat(newDB); os.IsNotExist(statErr2) {
					if dbRenameErr := os.Rename(oldDB, newDB); dbRenameErr != nil {
						fmt.Fprintf(os.Stderr, "[openclawder] DB rename failed (data may need manual migration): %v\n", dbRenameErr)
					}
				}
			}
			fmt.Fprintf(os.Stderr, "[openclawder] Migration complete. Original data preserved at %s\n", oldDir)
		}
	} else if oldErr == nil && oldInfo.IsDir() && newErr == nil {
		// Both exist — use new dir, warn about conflict.
		fmt.Fprintf(os.Stderr, "[openclawder] Note: legacy %s still exists alongside %s. Using %s. Remove the legacy directory when satisfied.\n", oldDir, newDir, newDir)
	}

	return newDir
}

// copyDir recursively copies src directory to dst.
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		return copyFile(path, target, info.Mode())
	})
}

// copyFile copies a single file from src to dst with the given mode.
func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// generateDirectoryID creates a stable directory ID based on path
// This is used for grouping instances in the same directory
func generateDirectoryID(directory string) string {
	hash := sha256.Sum256([]byte(directory))
	// Use first 16 bytes (32 hex chars) for a readable ID
	return hex.EncodeToString(hash[:16])
}
