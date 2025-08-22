// File: cmd/root.go
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	// Flags
	dataDir    string
	tendermint bool
	cosmosSdk  bool
	tx_idx     bool
	compact    bool
	versions   int
	blocks     int
	app        string

	// Logging flags
	verbose   bool
	batchLogs bool
	quiet     bool
)

// LogLevel represents different logging levels
type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

// Logger handles all output for the pruner
type Logger struct {
	level     LogLevel
	verbose   bool
	batchLogs bool
}

// Global logger
var logger *Logger

// NewLogger creates a new logger instance
func NewLogger(verbose, batchLogs bool) *Logger {
	level := LogLevelInfo
	if verbose {
		level = LogLevelDebug
	}

	return &Logger{
		level:     level,
		verbose:   verbose,
		batchLogs: batchLogs,
	}
}

// Info logs informational messages to stdout
func (l *Logger) Info(format string, args ...interface{}) {
	if l.level >= LogLevelInfo {
		message := fmt.Sprintf(format, args...)
		fmt.Fprintln(os.Stdout, message)
		os.Stdout.Sync() // Force flush for background processes
	}
}

// Error logs error messages to stderr
func (l *Logger) Error(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	os.Stderr.Sync() // Force flush for background processes
}

// Warn logs warning messages to stderr
func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level >= LogLevelWarn {
		message := fmt.Sprintf(format, args...)
		fmt.Fprintln(os.Stderr, "WARN:", message)
		os.Stderr.Sync()
	}
}

// Debug logs debug messages to stdout (only when verbose)
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level >= LogLevelDebug {
		message := fmt.Sprintf(format, args...)
		fmt.Fprintln(os.Stdout, "DEBUG:", message)
		os.Stdout.Sync()
	}
}

// BatchProgress logs batch processing progress (only when batchLogs enabled)
func (l *Logger) BatchProgress(operation string, count int, total ...int) {
	if l.batchLogs {
		var message string
		if len(total) > 0 && total[0] > 0 {
			message = fmt.Sprintf("[%s] processed batch %d (total processed: %d)", operation, count, total[0])
		} else {
			message = fmt.Sprintf("[%s] write batch %d", operation, count)
		}
		fmt.Fprintln(os.Stdout, message)
		os.Stdout.Sync()
	}
}

// Progress logs general progress messages
func (l *Logger) Progress(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stdout, message)
	os.Stdout.Sync()
}

func init() {
	rootCmd.AddCommand(pruneCmd())

	// Flags
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "data", "path to data directory")
	rootCmd.PersistentFlags().BoolVar(&tendermint, "tendermint", true, "prune tendermint data")
	rootCmd.PersistentFlags().BoolVar(&cosmosSdk, "cosmos-sdk", true, "prune cosmos-sdk data")
	rootCmd.PersistentFlags().BoolVar(&tx_idx, "tx_index", true, "prune tx_index")
	rootCmd.PersistentFlags().BoolVar(&compact, "compact", true, "compact dbs after pruning")
	rootCmd.PersistentFlags().IntVar(&versions, "versions", 10, "amount of versions to keep")
	rootCmd.PersistentFlags().IntVar(&blocks, "blocks", 10, "amount of blocks to keep")
	rootCmd.PersistentFlags().StringVar(&app, "app", "", "app name")

	// Logging flags
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "enable verbose/debug output")
	rootCmd.PersistentFlags().BoolVar(&batchLogs, "batch-logs", false, "enable batch processing logs (shows 'write batch' messages)")
	rootCmd.PersistentFlags().BoolVar(&quiet, "quiet", false, "suppress all non-error output")

	// Bind flags to viper
	viper.BindPFlag("data-dir", rootCmd.PersistentFlags().Lookup("data-dir"))
	viper.BindPFlag("tendermint", rootCmd.PersistentFlags().Lookup("tendermint"))
	viper.BindPFlag("cosmos-sdk", rootCmd.PersistentFlags().Lookup("cosmos-sdk"))
	viper.BindPFlag("tx_index", rootCmd.PersistentFlags().Lookup("tx_index"))
	viper.BindPFlag("compact", rootCmd.PersistentFlags().Lookup("compact"))
	viper.BindPFlag("versions", rootCmd.PersistentFlags().Lookup("versions"))
	viper.BindPFlag("blocks", rootCmd.PersistentFlags().Lookup("blocks"))
	viper.BindPFlag("app", rootCmd.PersistentFlags().Lookup("app"))
	viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("batch-logs", rootCmd.PersistentFlags().Lookup("batch-logs"))
	viper.BindPFlag("quiet", rootCmd.PersistentFlags().Lookup("quiet"))
}

var rootCmd = &cobra.Command{
	Use:   "cosmos-pruner",
	Short: "Prune cosmos node data",
	Long:  "A tool to prune cosmos node data including application state, block store, and tx index using LevelDB",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Initialize logger based on flags
		if quiet {
			// In quiet mode, only show errors
			logger = &Logger{level: LogLevelError, verbose: false, batchLogs: false}
		} else {
			logger = NewLogger(verbose, batchLogs)
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
