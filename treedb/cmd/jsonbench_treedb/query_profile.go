package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
)

type queryAttemptProfile struct {
	Attempt       int    `json:"attempt"`
	CPUProfile    string `json:"cpu_profile,omitempty"`
	AllocsProfile string `json:"allocs_profile,omitempty"`
}

type activeQueryProfile struct {
	attemptProfile queryAttemptProfile
	cpuFile        *os.File
}

func startQueryAttemptProfile(dir, query string, attempt int) (*activeQueryProfile, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create query profile dir: %w", err)
	}
	base := fmt.Sprintf("%s_attempt%d", sanitizeProfileName(query), attempt)
	cpuPath := filepath.Join(dir, "cpu_"+base+".pprof")
	cpuFile, err := os.Create(cpuPath)
	if err != nil {
		return nil, fmt.Errorf("create query cpu profile: %w", err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		_ = cpuFile.Close()
		_ = os.Remove(cpuPath)
		return nil, fmt.Errorf("start query cpu profile: %w", err)
	}
	return &activeQueryProfile{
		attemptProfile: queryAttemptProfile{
			Attempt:    attempt,
			CPUProfile: cpuPath,
		},
		cpuFile: cpuFile,
	}, nil
}

func (p *activeQueryProfile) Stop(dir, query string, attempt int) (queryAttemptProfile, error) {
	if p == nil {
		return queryAttemptProfile{}, nil
	}
	pprof.StopCPUProfile()
	var errs []error
	if err := p.cpuFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close query cpu profile: %w", err))
	}
	runtime.GC()
	base := fmt.Sprintf("%s_attempt%d", sanitizeProfileName(query), attempt)
	allocsPath := filepath.Join(dir, "allocs_"+base+".pprof")
	allocsFile, err := os.Create(allocsPath)
	if err != nil {
		errs = append(errs, fmt.Errorf("create query allocs profile: %w", err))
		return p.attemptProfile, errors.Join(errs...)
	}
	if profile := pprof.Lookup("allocs"); profile != nil {
		if err := profile.WriteTo(allocsFile, 0); err != nil {
			errs = append(errs, fmt.Errorf("write query allocs profile: %w", err))
		}
	}
	if err := allocsFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close query allocs profile: %w", err))
	}
	p.attemptProfile.AllocsProfile = allocsPath
	return p.attemptProfile, errors.Join(errs...)
}

func sanitizeProfileName(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return "query"
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "query"
	}
	return out
}
