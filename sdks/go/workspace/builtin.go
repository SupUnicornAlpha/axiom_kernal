package workspace

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type Request struct {
	Path    string   `json:"path"`
	Pattern string   `json:"pattern"`
	Old     string   `json:"old"`
	New     string   `json:"new"`
	Content string   `json:"content"`
	Argv    []string `json:"argv"`
}

func NewCodingRegistry(bashAllowlist [][]string) *Registry {
	r := NewRegistry()
	r.Register("list", func(root, input string) (string, error) {
		p, e := SecurePath(root, input, true)
		if e != nil {
			return "", e
		}
		entries, e := os.ReadDir(p)
		if e != nil {
			return "", e
		}
		names := []string{}
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		sort.Strings(names)
		return strings.Join(names, "\n"), nil
	})
	r.Register("read", func(root, input string) (string, error) {
		p, e := SecurePath(root, input, true)
		if e != nil {
			return "", e
		}
		b, e := os.ReadFile(p)
		return string(b), e
	})
	r.Register("grep", func(root, input string) (string, error) {
		var q Request
		if e := json.Unmarshal([]byte(input), &q); e != nil {
			return "", e
		}
		p, e := SecurePath(root, q.Path, true)
		if e != nil {
			return "", e
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return "", e
		}
		out := []string{}
		for i, line := range strings.Split(string(b), "\n") {
			if strings.Contains(line, q.Pattern) {
				out = append(out, fmt.Sprintf("%d:%s", i+1, line))
			}
		}
		return strings.Join(out, "\n"), nil
	})
	r.Register("edit", func(root, input string) (string, error) {
		var q Request
		if e := json.Unmarshal([]byte(input), &q); e != nil {
			return "", e
		}
		p, e := SecurePath(root, q.Path, true)
		if e != nil {
			return "", e
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return "", e
		}
		if strings.Count(string(b), q.Old) != 1 {
			return "", errors.New("edit requires exactly one match")
		}
		return p, os.WriteFile(p, []byte(strings.Replace(string(b), q.Old, q.New, 1)), 0644)
	})
	r.Register("write", func(root, input string) (string, error) {
		var q Request
		if e := json.Unmarshal([]byte(input), &q); e != nil {
			return "", e
		}
		p, e := SecurePath(root, q.Path, false)
		if e != nil {
			return "", e
		}
		if e = os.MkdirAll(filepath.Dir(p), 0755); e != nil {
			return "", e
		}
		return p, os.WriteFile(p, []byte(q.Content), 0644)
	})
	r.Register("bash", func(root, input string) (string, error) {
		var q Request
		if e := json.Unmarshal([]byte(input), &q); e != nil {
			return "", e
		}
		if !AllowedArgv(q.Argv, bashAllowlist) {
			return "", fmt.Errorf("bash command denied: %s", strings.Join(q.Argv, " "))
		}
		cmd := exec.Command(q.Argv[0], q.Argv[1:]...)
		cmd.Dir = root
		b, e := cmd.CombinedOutput()
		if e != nil {
			return fmt.Sprintf("exit_error: %v\n%s", e, b), nil
		}
		return string(b), nil
	})
	return r
}

func AllowedArgv(argv []string, allowlist [][]string) bool {
	for _, prefix := range allowlist {
		if len(argv) < len(prefix) {
			continue
		}
		ok := true
		for i, v := range prefix {
			if argv[i] != v {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
func SecurePath(root, relative string, existing bool) (string, error) {
	root, e := filepath.EvalSymlinks(root)
	if e != nil {
		return "", e
	}
	if filepath.IsAbs(relative) {
		return "", errors.New("absolute path denied")
	}
	clean := filepath.Clean(relative)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", errors.New("workspace escape denied")
	}
	candidate := filepath.Join(root, clean)
	check := candidate
	if !existing {
		check = filepath.Dir(candidate)
	}
	resolved, e := filepath.EvalSymlinks(check)
	if e != nil {
		return "", e
	}
	rel, e := filepath.Rel(root, resolved)
	if e != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", errors.New("symlink escape denied")
	}
	return candidate, nil
}
