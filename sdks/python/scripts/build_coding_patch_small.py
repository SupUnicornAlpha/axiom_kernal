import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from axiom_runtime_sdk_py import RunSpecBuilder


def build_coding_patch_small():
    reviewer = (
        RunSpecBuilder("reviewer-child", "reviewer child")
        .message_step("review-1", "review findings", "assistant", "patch looks safe")
        .message_step("review-2", "review verdict", "assistant", "approved")
        .build()
    )

    return (
        RunSpecBuilder("coding-patch-small", "coding patch small")
        .message_step("s1", "understand task", "user", "fix greeting output")
        .capability_step(
            "s2",
            "draft patch",
            "tool/write_patch",
            "replace hi with hello",
        )
        .delegate_step("s3", "delegate reviewer", reviewer, "append_messages")
        .capability_step("s4", "echo final result", "tool/echo", "hello")
        .lease("tool/write_patch")
        .lease("tool/echo")
        .build()
    )


if __name__ == "__main__":
    print(json.dumps(build_coding_patch_small(), indent=2))
