use axiom_spec::{
    Effect, Event, EventKind, MergeMode, Message, RunSpec, RunState, RunStatus, Step, StepAction,
};

use crate::{
    CapabilityContext, CapabilityRegistry, JsonlEventLog, Scheduler, Shell, ShellDecision,
};

#[derive(Debug)]
pub enum KernelError {
    Denied(String),
    Capability(String),
    BudgetExceeded(String),
}

#[derive(Clone, Debug)]
pub struct RunReport {
    pub state: RunState,
    pub events: Vec<Event>,
}

pub struct Kernel<S, H> {
    scheduler: S,
    shell: H,
    registry: CapabilityRegistry,
    event_log: Option<JsonlEventLog>,
}

impl<S, H> Kernel<S, H>
where
    S: Scheduler,
    H: Shell,
{
    pub fn new(
        scheduler: S,
        shell: H,
        registry: CapabilityRegistry,
        event_log: Option<JsonlEventLog>,
    ) -> Self {
        Self {
            scheduler,
            shell,
            registry,
            event_log,
        }
    }

    pub fn run(&self, spec: &RunSpec) -> Result<RunReport, KernelError> {
        let mut state = RunState::from_spec(spec);
        let mut events = Vec::new();

        self.push_event(
            &mut events,
            Event {
                run_id: spec.run_id.clone(),
                step_id: None,
                kind: EventKind::RunCreated,
                detail: spec.name.clone(),
            },
        );
        state.status = RunStatus::Running;
        self.push_event(
            &mut events,
            Event {
                run_id: spec.run_id.clone(),
                step_id: None,
                kind: EventKind::RunStarted,
                detail: "kernel_started".to_string(),
            },
        );

        while let Some(original_step) = self.scheduler.next(spec, state.next_step_index) {
            if state.next_step_index >= spec.budget.max_steps {
                state.status = RunStatus::Failed;
                let detail = format!("budget_exceeded:{}", spec.budget.max_steps);
                self.push_event(
                    &mut events,
                    Event {
                        run_id: spec.run_id.clone(),
                        step_id: Some(original_step.id.clone()),
                        kind: EventKind::RunFailed,
                        detail: detail.clone(),
                    },
                );
                return Err(KernelError::BudgetExceeded(detail));
            }

            self.push_event(
                &mut events,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: Some(original_step.id.clone()),
                    kind: EventKind::StepProposed,
                    detail: original_step.title.clone(),
                },
            );

            let step = match self.shell.before_step(spec, &state, original_step) {
                ShellDecision::Allow => {
                    self.push_event(
                        &mut events,
                        Event {
                            run_id: spec.run_id.clone(),
                            step_id: Some(original_step.id.clone()),
                            kind: EventKind::ShellDecision,
                            detail: "allow".to_string(),
                        },
                    );
                    original_step.clone()
                }
                ShellDecision::Rewrite { new_step, reason } => {
                    self.push_event(
                        &mut events,
                        Event {
                            run_id: spec.run_id.clone(),
                            step_id: Some(original_step.id.clone()),
                            kind: EventKind::ShellDecision,
                            detail: format!("rewrite:{reason}"),
                        },
                    );
                    new_step
                }
                ShellDecision::Deny { reason } => {
                    state.denied_actions.push(original_step.id.clone());
                    self.push_event(
                        &mut events,
                        Event {
                            run_id: spec.run_id.clone(),
                            step_id: Some(original_step.id.clone()),
                            kind: EventKind::ShellDecision,
                            detail: format!("deny:{reason}"),
                        },
                    );
                    state.next_step_index += 1;
                    continue;
                }
            };

            self.push_event(
                &mut events,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: Some(step.id.clone()),
                    kind: EventKind::StepStarted,
                    detail: step.title.clone(),
                },
            );

            let effect = self.execute_step(spec, &state, &step, &mut events)?;
            self.push_event(
                &mut events,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: Some(step.id.clone()),
                    kind: EventKind::StepCompleted,
                    detail: effect.summary.clone(),
                },
            );
            apply_effect(&mut state, effect.clone());
            self.push_event(
                &mut events,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: Some(step.id.clone()),
                    kind: EventKind::EffectApplied,
                    detail: effect.summary,
                },
            );

            state.next_step_index += 1;
        }

        state.status = RunStatus::Completed;
        self.push_event(
            &mut events,
            Event {
                run_id: spec.run_id.clone(),
                step_id: None,
                kind: EventKind::RunCompleted,
                detail: "ok".to_string(),
            },
        );

        Ok(RunReport { state, events })
    }

    fn execute_step(
        &self,
        spec: &RunSpec,
        state: &RunState,
        step: &Step,
        events: &mut Vec<Event>,
    ) -> Result<Effect, KernelError> {
        match &step.action {
            StepAction::Message { role, content } => Ok(Effect {
                summary: format!("message:{role}"),
                messages: vec![Message {
                    role: role.clone(),
                    content: content.clone(),
                }],
                outputs: Vec::new(),
            }),
            StepAction::CapabilityInvoke {
                capability_id,
                input,
            } => {
                if !CapabilityRegistry::is_leased(&spec.capability_leases, capability_id) {
                    let detail = format!("capability_denied:{capability_id}");
                    self.push_event(
                        events,
                        Event {
                            run_id: spec.run_id.clone(),
                            step_id: Some(step.id.clone()),
                            kind: EventKind::CapabilityDenied,
                            detail: detail.clone(),
                        },
                    );
                    return Err(KernelError::Denied(detail));
                }

                let ctx = CapabilityContext {
                    run_spec: spec,
                    run_state: state,
                };
                self.registry
                    .invoke(capability_id, input, &ctx)
                    .map_err(KernelError::Capability)
            }
            StepAction::Delegate { child, merge_mode } => {
                for child_lease in &child.capability_leases {
                    if !CapabilityRegistry::is_leased(&spec.capability_leases, &child_lease.capability_id)
                    {
                        let detail =
                            format!("child_capability_not_delegated:{}", child_lease.capability_id);
                        self.push_event(
                            events,
                            Event {
                                run_id: spec.run_id.clone(),
                                step_id: Some(step.id.clone()),
                                kind: EventKind::CapabilityDenied,
                                detail: detail.clone(),
                            },
                        );
                        return Err(KernelError::Denied(detail));
                    }
                }
                self.push_event(
                    events,
                    Event {
                        run_id: spec.run_id.clone(),
                        step_id: Some(step.id.clone()),
                        kind: EventKind::ChildRunCreated,
                        detail: child.run_id.clone(),
                    },
                );
                let child_report = self.run(child)?;
                self.push_event(
                    events,
                    Event {
                        run_id: spec.run_id.clone(),
                        step_id: Some(step.id.clone()),
                        kind: EventKind::ChildRunCompleted,
                        detail: format!("{}:{:?}", child.run_id, child_report.state.status),
                    },
                );

                let mut effect = Effect::empty(format!("child_run:{}", child.run_id));
                match merge_mode {
                    MergeMode::SummaryOnly => {
                        effect.outputs.push(format!(
                            "child_summary:{}:{} outputs",
                            child.run_id,
                            child_report.state.outputs.len()
                        ));
                    }
                    MergeMode::AppendMessages => {
                        effect.messages.extend(child_report.state.messages);
                        effect.outputs.extend(child_report.state.outputs);
                    }
                }
                Ok(effect)
            }
        }
    }

    fn push_event(&self, events: &mut Vec<Event>, event: Event) {
        if let Some(event_log) = &self.event_log {
            let _ = event_log.append(&event);
        }
        events.push(event);
    }
}

fn apply_effect(state: &mut RunState, effect: Effect) {
    state.messages.extend(effect.messages);
    state.outputs.extend(effect.outputs);
}
