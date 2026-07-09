use axiom_spec::{RunSpec, RunState, Step};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShellDecision {
    Allow,
    Rewrite { new_step: Step, reason: String },
    Deny { reason: String },
}

pub trait Shell {
    fn before_step(&self, spec: &RunSpec, state: &RunState, step: &Step) -> ShellDecision;
}

#[derive(Default)]
pub struct AuditShell;

impl Shell for AuditShell {
    fn before_step(&self, _spec: &RunSpec, _state: &RunState, step: &Step) -> ShellDecision {
        if step.title.contains("[deny]") {
            return ShellDecision::Deny {
                reason: "title_matched_deny_rule".to_string(),
            };
        }

        if step.title.contains("[rewrite]") {
            let mut rewritten = step.clone();
            rewritten.title = rewritten.title.replace("[rewrite]", "[rewritten]");
            return ShellDecision::Rewrite {
                new_step: rewritten,
                reason: "title_matched_rewrite_rule".to_string(),
            };
        }

        ShellDecision::Allow
    }
}
