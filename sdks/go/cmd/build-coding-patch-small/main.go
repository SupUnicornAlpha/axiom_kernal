package main

import (
	"encoding/json"
	"os"

	axiom "github.com/SupUnicornAlpha/axiom_kernal/sdks/go"
)

func main() {
	reviewer := axiom.NewRunSpec("reviewer-child", "reviewer child").
		MessageStep("review-1", "review findings", "assistant", "patch looks safe").
		MessageStep("review-2", "review verdict", "assistant", "approved").Build()
	spec := axiom.NewRunSpec("coding-patch-small", "coding patch small").
		MessageStep("s1", "understand task", "user", "fix greeting output").
		CapabilityStep("s2", "draft patch", "tool/write_patch", "replace hi with hello").
		DelegateStep("s3", "delegate reviewer", reviewer, "append_messages").
		CapabilityStep("s4", "echo final result", "tool/echo", "hello").
		Lease("tool/write_patch").Lease("tool/echo").Build()
	_ = json.NewEncoder(os.Stdout).Encode(spec)
}
