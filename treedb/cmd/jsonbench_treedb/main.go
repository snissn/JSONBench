package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

const schemaVersion = "jsonbench-treedb/v1"

func main() {
	if err := runCLI(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "jsonbench_treedb: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(argv []string) error {
	if len(argv) < 2 {
		usage(os.Stderr)
		return errors.New("missing command")
	}
	switch argv[1] {
	case "run":
		cfg, err := parseRunFlags(argv[2:])
		if err != nil {
			return err
		}
		result, err := runTreeDBBenchmark(cfg)
		if err != nil {
			return err
		}
		return writeJSON(cfg.Out, result)
	case "report":
		cfg, err := parseReportFlags(argv[2:])
		if err != nil {
			return err
		}
		return writeReport(cfg)
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	default:
		usage(os.Stderr)
		return fmt.Errorf("unknown command %q", argv[1])
	}
}

func usage(out *os.File) {
	fmt.Fprintln(out, "Usage:")
	fmt.Fprintln(out, "  jsonbench_treedb run [flags]")
	fmt.Fprintln(out, "  jsonbench_treedb report [flags]")
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if strings.TrimSpace(path) == "" || path == "-" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err := os.MkdirAll(parentDir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func parentDir(path string) string {
	idx := strings.LastIndexAny(path, `/\`)
	if idx < 0 {
		return "."
	}
	return path[:idx]
}

func flagUsage(fs *flag.FlagSet, summary string) {
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), summary)
		fs.PrintDefaults()
	}
}
