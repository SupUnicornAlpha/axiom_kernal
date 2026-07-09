use axiom_spec::{
    Effect, Event, EventKind, MergeMode, Message, RunSpec, RunState, RunStatus, Step, StepAction,
};

use crate::{
    CapabilityRegistry, CapabilityTransport, EventBus, InMemoryEventBus, JsonlEventLog, RunStore,
    RunStoreRecord, Scheduler, Shell, ShellDecision,
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

pub struct Kernel<S, H, T> {
    scheduler: S,
    shell: H,
    transport: T,
    event_log: Option<JsonlEventLog>,
}

impl<S, H, T> Kernel<S, H, T>
where
    S: Scheduler,
    H: Shell,
    T: CapabilityTransport,
{
    pub fn new(
        scheduler: S,
        shell: H,
        transport: T,
        event_log: Option<JsonlEventLog>,
    ) -> Self {
        Self {
            scheduler,
            shell,
            transport,
            event_log,
        }
    }

    pub fn run(&self, spec: &RunSpec) -> Result<RunReport, KernelError> {
        let mut state = RunState::from_spec(spec);
        let mut events = Vec::new();
        let mut event_bus = InMemoryEventBus::new();

        self.push_event(
            &mut events,
            &mut event_bus,
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
            &mut event_bus,
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
                    &mut event_bus,
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
                &mut event_bus,
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
                        &mut event_bus,
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
                        &mut event_bus,
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
                        &mut event_bus,
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
                &mut event_bus,
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
                &mut event_bus,
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
                &mut event_bus,
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
            &mut event_bus,
            Event {
                run_id: spec.run_id.clone(),
                step_id: None,
                kind: EventKind::RunCompleted,
                detail: "ok".to_string(),
            },
        );

        let mut run_store = crate::MemoryRunStore::new();
        run_store.put(RunStoreRecord {
            run_id: state.run_id.clone(),
            state: state.clone(),
        });

        let _ = run_store.get(&state.run_id);
        let _ = event_bus.snapshot();

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
                self.transport
                    .invoke(&spec.capability_leases, capability_id, input, spec, state)
                    .map_err(|detail| {
                        if detail.starts_with("capability_denied:") {
                            self.push_event(
                                events,
                                &mut InMemoryEventBus::new(),
                                Event {
                                    run_id: spec.run_id.clone(),
                                    step_id: Some(step.id.clone()),
                                    kind: EventKind::CapabilityDenied,
                                    detail: detail.clone(),
                                },
                            );
                            KernelError::Denied(detail)
                        } else {
                            KernelError::Capability(detail)
                        }
                    })
            }
            StepAction::Delegate { child, merge_mode } => {
                for child_lease in &child.capability_leases {
                    if !CapabilityRegistry::is_leased(&spec.capability_leases, &child_lease.capability_id)
                    {
                        let detail =
                            format!("child_capability_not_delegated:{}", child_lease.capability_id);
                        self.push_event(
                            events,
                            &mut InMemoryEventBus::new(),
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
                    &mut InMemoryEventBus::new(),
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
                    &mut InMemoryEventBus::new(),
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

    fn push_event(&self, events: &mut Vec<Event>, event_bus: &mut dyn EventBus, event: Event) {
        if let Some(event_log) = &self.event_log {
            let _ = event_log.append(&event);
        }
        event_bus.publish(event.clone());
        events.push(event);
    }
}

fn apply_effect(state: &mut RunState, effect: Effect) {
    state.messages.extend(effect.messages);
    state.outputs.extend(effect.outputs);
}
