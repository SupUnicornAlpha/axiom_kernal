use std::sync::Arc;

use axiom_spec::{
    Effect, EffectProposal, Event, EventKind, MergeMode, Message, RunSpec, RunState, RunStatus,
    Step, StepAction,
};

use crate::{
    CapabilityTransport, EventBus, EventJournal, InMemoryEventBus, JsonlEventLog,
    LocalSubRunTransport, MemoryRunStore, RunStore, RunStoreRecord, Scheduler, Shell,
    ShellDecision, SubRunTransport,
};

#[derive(Debug)]
pub enum KernelError {
    Denied(String),
    Capability(String),
    BudgetExceeded(String),
    EventLog(String),
    RunStore(String),
    MissingCheckpoint(String),
}

#[derive(Clone, Debug)]
pub struct RunReport {
    pub state: RunState,
    pub events: Vec<Event>,
}

pub struct Kernel<S, H, T, R = LocalSubRunTransport> {
    scheduler: S,
    shell: H,
    transport: T,
    subrun_transport: R,
    event_log: Option<Arc<dyn EventJournal>>,
    run_store: Arc<dyn RunStore>,
}

impl<S, H, T> Kernel<S, H, T, LocalSubRunTransport>
where
    S: Scheduler,
    H: Shell,
    T: CapabilityTransport,
{
    pub fn new(scheduler: S, shell: H, transport: T, event_log: Option<JsonlEventLog>) -> Self {
        Self {
            scheduler,
            shell,
            transport,
            subrun_transport: LocalSubRunTransport,
            event_log: event_log.map(|journal| Arc::new(journal) as Arc<dyn EventJournal>),
            run_store: Arc::new(MemoryRunStore::new()),
        }
    }
}

impl<S, H, T, R> Kernel<S, H, T, R>
where
    S: Scheduler,
    H: Shell,
    T: CapabilityTransport,
    R: SubRunTransport,
{
    pub fn with_subrun_transport(
        scheduler: S,
        shell: H,
        transport: T,
        subrun_transport: R,
        event_log: Option<JsonlEventLog>,
    ) -> Self {
        Self {
            scheduler,
            shell,
            transport,
            subrun_transport,
            event_log: event_log.map(|journal| Arc::new(journal) as Arc<dyn EventJournal>),
            run_store: Arc::new(MemoryRunStore::new()),
        }
    }

    pub fn with_services(
        scheduler: S,
        shell: H,
        transport: T,
        subrun_transport: R,
        event_log: Option<Arc<dyn EventJournal>>,
        run_store: Arc<dyn RunStore>,
    ) -> Self {
        Self {
            scheduler,
            shell,
            transport,
            subrun_transport,
            event_log,
            run_store,
        }
    }

    pub fn run(&self, spec: &RunSpec) -> Result<RunReport, KernelError> {
        self.run_from_state(spec, RunState::from_spec(spec), false)
    }

    pub fn resume(&self, spec: &RunSpec) -> Result<RunReport, KernelError> {
        let checkpoint = self
            .run_store
            .get(&spec.run_id)
            .map_err(KernelError::RunStore)?
            .ok_or_else(|| KernelError::MissingCheckpoint(spec.run_id.clone()))?;
        self.run_from_state(spec, checkpoint.state, true)
    }

    fn run_from_state(
        &self,
        spec: &RunSpec,
        mut state: RunState,
        resumed: bool,
    ) -> Result<RunReport, KernelError> {
        let mut events = Vec::new();
        let mut event_bus = InMemoryEventBus::new();

        if !resumed {
            self.push_event(
                &mut events,
                &mut event_bus,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: None,
                    kind: EventKind::RunCreated,
                    detail: spec.name.clone(),
                },
            )?;
        }
        state.status = RunStatus::Running;
        self.push_event(
            &mut events,
            &mut event_bus,
            Event {
                run_id: spec.run_id.clone(),
                step_id: None,
                kind: EventKind::RunStarted,
                detail: if resumed {
                    "kernel_resumed".to_string()
                } else {
                    "kernel_started".to_string()
                },
            },
        )?;
        self.checkpoint(&state)?;

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
                )?;
                self.checkpoint(&state)?;
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
            )?;

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
                    )?;
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
                    )?;
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
                    )?;
                    state.next_step_index += 1;
                    self.checkpoint(&state)?;
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
            )?;

            let proposal = self.execute_step(spec, &state, &step, &mut events)?;
            self.push_event(
                &mut events,
                &mut event_bus,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: Some(step.id.clone()),
                    kind: EventKind::StepCompleted,
                    detail: proposal.summary.clone(),
                },
            )?;
            self.push_event(
                &mut events,
                &mut event_bus,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: Some(step.id.clone()),
                    kind: EventKind::EffectProposed,
                    detail: proposal.summary.clone(),
                },
            )?;
            let effect: Effect = proposal.into();
            self.push_event(
                &mut events,
                &mut event_bus,
                Event {
                    run_id: spec.run_id.clone(),
                    step_id: Some(step.id.clone()),
                    kind: EventKind::EffectCommitted,
                    detail: effect.summary.clone(),
                },
            )?;
            apply_effect(&mut state, effect);

            state.next_step_index += 1;
            self.checkpoint(&state)?;
        }

        self.push_event(
            &mut events,
            &mut event_bus,
            Event {
                run_id: spec.run_id.clone(),
                step_id: None,
                kind: EventKind::RunCompleted,
                detail: "ok".to_string(),
            },
        )?;
        state.status = RunStatus::Completed;
        self.checkpoint(&state)?;

        let _ = event_bus.snapshot();

        Ok(RunReport { state, events })
    }

    fn execute_step(
        &self,
        spec: &RunSpec,
        state: &RunState,
        step: &Step,
        events: &mut Vec<Event>,
    ) -> Result<EffectProposal, KernelError> {
        match &step.action {
            StepAction::Message { role, content } => Ok(EffectProposal {
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
            } => match self.transport.invoke(
                &spec.capability_leases,
                capability_id,
                input,
                spec,
                state,
            ) {
                Ok(proposal) => Ok(proposal),
                Err(detail) => {
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
                        )?;
                        Err(KernelError::Denied(detail))
                    } else {
                        Err(KernelError::Capability(detail))
                    }
                }
            },
            StepAction::Delegate { child, merge_mode } => {
                if let Err(detail) = validate_child_run(spec, child) {
                    self.push_event(
                        events,
                        &mut InMemoryEventBus::new(),
                        Event {
                            run_id: spec.run_id.clone(),
                            step_id: Some(step.id.clone()),
                            kind: EventKind::CapabilityDenied,
                            detail: detail.clone(),
                        },
                    )?;
                    return Err(KernelError::Denied(detail));
                }
                self.push_event(
                    events,
                    &mut InMemoryEventBus::new(),
                    Event {
                        run_id: spec.run_id.clone(),
                        step_id: Some(step.id.clone()),
                        kind: EventKind::ChildRunCreated,
                        detail: child.run.run_id.clone(),
                    },
                )?;
                let child_result = self
                    .subrun_transport
                    .execute(child, &|child_spec| self.run(child_spec))?;
                self.push_event(
                    events,
                    &mut InMemoryEventBus::new(),
                    Event {
                        run_id: spec.run_id.clone(),
                        step_id: Some(step.id.clone()),
                        kind: EventKind::ChildRunCompleted,
                        detail: format!(
                            "{}:{:?}:{}",
                            child_result.run_id, child_result.status, child_result.events_digest
                        ),
                    },
                )?;

                let mut proposal =
                    EffectProposal::empty(format!("child_run:{}", child_result.run_id));
                match merge_mode {
                    MergeMode::SummaryOnly => {
                        proposal.outputs.push(format!(
                            "child_summary:{}:{} proposed_effects",
                            child_result.run_id,
                            child_result.proposed_effects.len()
                        ));
                    }
                    MergeMode::AppendMessages => {
                        for proposed_effect in child_result.proposed_effects {
                            proposal.messages.extend(proposed_effect.messages);
                            proposal.outputs.extend(proposed_effect.outputs);
                        }
                    }
                }
                Ok(proposal)
            }
        }
    }

    fn push_event(
        &self,
        events: &mut Vec<Event>,
        event_bus: &mut dyn EventBus,
        event: Event,
    ) -> Result<(), KernelError> {
        if let Some(event_log) = &self.event_log {
            event_log.append(&event).map_err(KernelError::EventLog)?;
        }
        event_bus.publish(event.clone());
        events.push(event);
        Ok(())
    }

    fn checkpoint(&self, state: &RunState) -> Result<(), KernelError> {
        self.run_store
            .put(RunStoreRecord {
                run_id: state.run_id.clone(),
                state: state.clone(),
            })
            .map_err(KernelError::RunStore)
    }
}

fn validate_child_run(parent: &RunSpec, child: &axiom_spec::ChildRunSpec) -> Result<(), String> {
    if child.parent_run_id != parent.run_id {
        return Err(format!(
            "child_parent_mismatch:{}:{}",
            child.parent_run_id, parent.run_id
        ));
    }
    if child.run.budget.max_steps > parent.budget.max_steps {
        return Err(format!(
            "child_budget_exceeds_parent:{}:{}",
            child.run.budget.max_steps, parent.budget.max_steps
        ));
    }
    if !std::path::Path::new(&child.run.namespace.workspace_root)
        .starts_with(std::path::Path::new(&parent.namespace.workspace_root))
    {
        return Err(format!(
            "child_namespace_outside_parent:{}",
            child.run.namespace.workspace_root
        ));
    }
    for capability_id in &child.run.namespace.visible_capabilities {
        if !parent
            .namespace
            .visible_capabilities
            .contains(capability_id)
        {
            return Err(format!(
                "child_namespace_capability_not_visible:{capability_id}"
            ));
        }
    }
    for child_lease in &child.run.capability_leases {
        let Some(parent_lease) = parent
            .capability_leases
            .iter()
            .find(|lease| lease.capability_id == child_lease.capability_id)
        else {
            return Err(format!(
                "child_capability_not_delegated:{}",
                child_lease.capability_id
            ));
        };
        for permission in &child_lease.permissions {
            if !parent_lease.permissions.contains(permission) {
                return Err(format!(
                    "child_permission_not_delegated:{}:{}",
                    child_lease.capability_id, permission
                ));
            }
        }
    }
    Ok(())
}

fn apply_effect(state: &mut RunState, effect: Effect) {
    state.messages.extend(effect.messages);
    state.outputs.extend(effect.outputs);
}
