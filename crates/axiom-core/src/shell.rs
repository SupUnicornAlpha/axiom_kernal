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

pub trait ShellMiddleware {
    fn handle(&self, spec: &RunSpec, state: &RunState, step: &Step) -> Option<ShellDecision>;
}

#[derive(Default)]
pub struct CompositeShell {
    middlewares: Vec<Box<dyn ShellMiddleware + Send + Sync>>,
}

impl CompositeShell {
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    pub fn push<M>(&mut self, middleware: M)
    where
        M: ShellMiddleware + Send + Sync + 'static,
    {
        self.middlewares.push(Box::new(middleware));
    }
}

impl Shell for CompositeShell {
    fn before_step(&self, spec: &RunSpec, state: &RunState, step: &Step) -> ShellDecision {
        let mut current_step = step.clone();
        let mut rewrite_reasons = Vec::new();

        for middleware in &self.middlewares {
            if let Some(decision) = middleware.handle(spec, state, &current_step) {
                match decision {
                    ShellDecision::Allow => {}
                    ShellDecision::Rewrite { new_step, reason } => {
                        current_step = new_step;
                        rewrite_reasons.push(reason);
                    }
                    ShellDecision::Deny { reason } => {
                        return ShellDecision::Deny { reason };
                    }
                }
            }
        }

        if rewrite_reasons.is_empty() {
            ShellDecision::Allow
        } else {
            ShellDecision::Rewrite {
                new_step: current_step,
                reason: rewrite_reasons.join("+"),
            }
        }
    }
}

pub trait PolicyEngine {
    fn evaluate(&self, spec: &RunSpec, state: &RunState, step: &Step) -> Option<ShellDecision>;
}

#[derive(Default)]
pub struct MinimalPolicyEngine {
    denied_capabilities: Vec<String>,
    denied_title_terms: Vec<String>,
}

impl MinimalPolicyEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn deny_capability(mut self, capability_id: impl Into<String>) -> Self {
        self.denied_capabilities.push(capability_id.into());
        self
    }

    pub fn deny_title_term(mut self, term: impl Into<String>) -> Self {
        self.denied_title_terms.push(term.into());
        self
    }
}

impl PolicyEngine for MinimalPolicyEngine {
    fn evaluate(&self, _spec: &RunSpec, _state: &RunState, step: &Step) -> Option<ShellDecision> {
        for denied_term in &self.denied_title_terms {
            if step.title.contains(denied_term) {
                return Some(ShellDecision::Deny {
                    reason: format!("policy_denied_title_term:{denied_term}"),
                });
            }
        }

        if let axiom_spec::StepAction::CapabilityInvoke { capability_id, .. } = &step.action {
            if self
                .denied_capabilities
                .iter()
                .any(|denied| denied == capability_id)
            {
                return Some(ShellDecision::Deny {
                    reason: format!("policy_denied_capability:{capability_id}"),
                });
            }
        }

        None
    }
}

pub struct PolicyMiddleware<P> {
    engine: P,
}

impl<P> PolicyMiddleware<P> {
    pub fn new(engine: P) -> Self {
        Self { engine }
    }
}

impl<P> ShellMiddleware for PolicyMiddleware<P>
where
    P: PolicyEngine + Send + Sync,
{
    fn handle(&self, spec: &RunSpec, state: &RunState, step: &Step) -> Option<ShellDecision> {
        self.engine.evaluate(spec, state, step)
    }
}

#[derive(Default)]
pub struct TitlePolicyMiddleware;

impl ShellMiddleware for TitlePolicyMiddleware {
    fn handle(&self, _spec: &RunSpec, _state: &RunState, step: &Step) -> Option<ShellDecision> {
        if step.title.contains("[deny]") {
            return Some(ShellDecision::Deny {
                reason: "title_matched_deny_rule".to_string(),
            });
        }

        if step.title.contains("[rewrite]") {
            let mut rewritten = step.clone();
            rewritten.title = rewritten.title.replace("[rewrite]", "[rewritten]");
            return Some(ShellDecision::Rewrite {
                new_step: rewritten,
                reason: "title_matched_rewrite_rule".to_string(),
            });
        }

        None
    }
}

#[derive(Default)]
pub struct AuditShell;

impl Shell for AuditShell {
    fn before_step(&self, _spec: &RunSpec, _state: &RunState, step: &Step) -> ShellDecision {
        TitlePolicyMiddleware
            .handle(_spec, _state, step)
            .unwrap_or(ShellDecision::Allow)
    }
}
