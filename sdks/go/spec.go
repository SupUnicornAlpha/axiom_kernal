package axiom

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

const RunSpecSchemaVersion = 1

type RunSpec struct {
	SchemaVersion    uint32            `json:"schema_version"`
	RunID            string            `json:"run_id"`
	Name             string            `json:"name"`
	Namespace        RunNamespace      `json:"namespace"`
	Budget           BudgetGroup       `json:"budget"`
	CapabilityLeases []CapabilityLease `json:"capability_leases"`
	Steps            []Step            `json:"steps"`
	Metadata         map[string]string `json:"metadata"`
}
type RunNamespace struct {
	WorkspaceRoot       string   `json:"workspace_root"`
	VisibleCapabilities []string `json:"visible_capabilities"`
}
type BudgetGroup struct {
	MaxSteps int `json:"max_steps"`
}
type CapabilityLease struct {
	CapabilityID string   `json:"capability_id"`
	Permissions  []string `json:"permissions"`
}
type Step struct {
	ID     string     `json:"id"`
	Title  string     `json:"title"`
	Action StepAction `json:"action"`
}
type StepAction struct {
	Kind         string        `json:"kind"`
	Role         string        `json:"role,omitempty"`
	Content      string        `json:"content,omitempty"`
	CapabilityID string        `json:"capability_id,omitempty"`
	Input        string        `json:"input,omitempty"`
	Child        *ChildRunSpec `json:"child,omitempty"`
	MergeMode    string        `json:"merge_mode,omitempty"`
}
type ChildRunSpec struct {
	ParentRunID    string   `json:"parent_run_id"`
	Run            RunSpec  `json:"run"`
	MemoryView     []string `json:"memory_view"`
	SandboxProfile string   `json:"sandbox_profile"`
	ReturnContract string   `json:"return_contract"`
}

type RunSpecBuilder struct{ spec RunSpec }

func NewRunSpec(runID, name string) *RunSpecBuilder {
	return &RunSpecBuilder{spec: RunSpec{SchemaVersion: 1, RunID: runID, Name: name, Namespace: RunNamespace{WorkspaceRoot: ".", VisibleCapabilities: []string{}}, Budget: BudgetGroup{MaxSteps: 128}, CapabilityLeases: []CapabilityLease{}, Steps: []Step{}, Metadata: map[string]string{}}}
}
func (b *RunSpecBuilder) WorkspaceRoot(v string) *RunSpecBuilder {
	b.spec.Namespace.WorkspaceRoot = v
	return b
}
func (b *RunSpecBuilder) BudgetMaxSteps(v int) *RunSpecBuilder { b.spec.Budget.MaxSteps = v; return b }
func (b *RunSpecBuilder) VisibleCapability(id string) *RunSpecBuilder {
	b.spec.Namespace.VisibleCapabilities = append(b.spec.Namespace.VisibleCapabilities, id)
	return b
}
func (b *RunSpecBuilder) Lease(id string, permissions ...string) *RunSpecBuilder {
	if len(permissions) == 0 {
		permissions = []string{"invoke"}
	}
	b.spec.CapabilityLeases = append(b.spec.CapabilityLeases, CapabilityLease{CapabilityID: id, Permissions: permissions})
	return b
}
func (b *RunSpecBuilder) MessageStep(id, title, role, content string) *RunSpecBuilder {
	b.spec.Steps = append(b.spec.Steps, Step{ID: id, Title: title, Action: StepAction{Kind: "message", Role: role, Content: content}})
	return b
}
func (b *RunSpecBuilder) CapabilityStep(id, title, capabilityID, input string) *RunSpecBuilder {
	b.spec.Steps = append(b.spec.Steps, Step{ID: id, Title: title, Action: StepAction{Kind: "capability_invoke", CapabilityID: capabilityID, Input: input}})
	return b
}
func (b *RunSpecBuilder) DelegateStep(id, title string, child RunSpec, mergeMode string) *RunSpecBuilder {
	b.spec.Steps = append(b.spec.Steps, Step{ID: id, Title: title, Action: StepAction{Kind: "delegate", Child: &ChildRunSpec{ParentRunID: b.spec.RunID, Run: child, MemoryView: []string{}, SandboxProfile: "default-deny", ReturnContract: "effect-proposals-v1"}, MergeMode: mergeMode}})
	return b
}
func (b *RunSpecBuilder) Metadata(key, value string) *RunSpecBuilder {
	b.spec.Metadata[key] = value
	return b
}
func (b *RunSpecBuilder) Build() RunSpec {
	raw, _ := json.Marshal(b.spec)
	var result RunSpec
	_ = json.Unmarshal(raw, &result)
	return result
}
func (s RunSpec) Digest() (string, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
