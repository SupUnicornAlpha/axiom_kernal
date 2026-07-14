package axiom

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type Decision struct {
	Kind         string `json:"kind"`
	CapabilityID string `json:"capability_id,omitempty"`
	Input        string `json:"input,omitempty"`
	Content      string `json:"content,omitempty"`
}
type ObservationMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type Observation struct {
	Task                string               `json:"task"`
	Messages            []ObservationMessage `json:"messages"`
	Outputs             []string             `json:"outputs"`
	DeniedActions       []string             `json:"denied_actions"`
	NextStepIndex       int                  `json:"next_step_index"`
	VisibleCapabilities []string             `json:"visible_capabilities"`
	WorkspaceRoot       string               `json:"workspace_root,omitempty"`
}
type Planner interface {
	Plan(task string) ([]Decision, error)
	Decide(Observation) (Decision, error)
}
type ToolHost interface {
	Invoke(name, root, permission, input string) (string, error)
}
type Sidecar struct {
	SystemPrompt string
	Planner      Planner
	Tools        ToolHost
}

func (s Sidecar) Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: prompt|plan|decide|tool")
		return 2
	}
	var err error
	switch args[0] {
	case "prompt":
		_, err = fmt.Fprint(stdout, s.SystemPrompt)
	case "plan":
		task := ""
		if len(args) > 1 {
			task = args[1]
		}
		var decisions []Decision
		decisions, err = s.Planner.Plan(task)
		if err == nil {
			err = json.NewEncoder(stdout).Encode(decisions)
		}
	case "decide":
		var raw []byte
		if len(args) > 1 && args[1] != "-" {
			raw, err = os.ReadFile(args[1])
		} else {
			raw, err = io.ReadAll(stdin)
		}
		if err == nil {
			var obs Observation
			err = json.Unmarshal(raw, &obs)
			if err == nil {
				var d Decision
				d, err = s.Planner.Decide(obs)
				if err == nil {
					err = json.NewEncoder(stdout).Encode(d)
				}
			}
		}
	case "tool":
		if len(args) != 5 {
			err = errors.New("tool requires name root permission input")
		} else {
			var output string
			output, err = s.Tools.Invoke(args[1], args[2], args[3], args[4])
			if err == nil {
				_, err = fmt.Fprint(stdout, output)
			}
		}
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func ValidateDecision(d Decision, capabilityPrefix string) error {
	switch d.Kind {
	case "invoke":
		if d.CapabilityID == "" || !strings.HasPrefix(d.CapabilityID, capabilityPrefix) {
			return errors.New("invalid capability")
		}
	case "respond":
		if strings.TrimSpace(d.Content) == "" {
			return errors.New("empty response")
		}
	case "finish":
	default:
		return errors.New("unknown decision kind")
	}
	return nil
}
