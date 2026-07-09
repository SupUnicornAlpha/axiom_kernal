import { RunSpecBuilder } from "../src/index.mjs"

function buildCodingPatchSmall() {
  const reviewer = new RunSpecBuilder("reviewer-child", "reviewer child")
    .messageStep("review-1", "review findings", "assistant", "patch looks safe")
    .messageStep("review-2", "review verdict", "assistant", "approved")
    .build()

  return new RunSpecBuilder("coding-patch-small", "coding patch small")
    .messageStep("s1", "understand task", "user", "fix greeting output")
    .capabilityStep("s2", "draft patch", "tool/write_patch", "replace hi with hello")
    .delegateStep("s3", "delegate reviewer", reviewer, "append_messages")
    .capabilityStep("s4", "echo final result", "tool/echo", "hello")
    .lease("tool/write_patch")
    .lease("tool/echo")
    .build()
}

console.log(JSON.stringify(buildCodingPatchSmall(), null, 2))
