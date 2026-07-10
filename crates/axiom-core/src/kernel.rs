use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[derive(Clone, Debug, Default)]
struct CommitCursor {
    last_sequence: u64,
    applied_commit_ids: BTreeSet<String>,
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
        self.run_from_state(
            spec,
            RunState::from_spec(spec),
            CommitCursor::default(),
            false,
        )
    }

    pub fn resume(&self, spec: &RunSpec) -> Result<RunReport, KernelError> {
        let mut checkpoint = self
            .run_store
            .get(&spec.run_id)
            .map_err(KernelError::RunStore)?
            .ok_or_else(|| KernelError::MissingCheckpoint(spec.run_id.clone()))?;
        let spec_digest = spec.digest();
        if checkpoint.spec_digest != spec_digest {
            return Err(KernelError::RunStore(format!(
                "checkpoint_spec_digest_mismatch:{}:{}",
                checkpoint.spec_digest, spec_digest
            )));
        }
        if checkpoint.checkpoint_version != 1 {
            return Err(KernelError::RunStore(format!(
                "unsupported_checkpoint_version:{}",
                checkpoint.checkpoint_version
            )));
        }
        let recovered_events = self.recover_from_journal(spec, &mut checkpoint)?;
        self.run_store
            .put(checkpoint.clone())
            .map_err(KernelError::RunStore)?;
        if checkpoint.state.status == RunStatus::Completed {
            return Ok(RunReport {
                state: checkpoint.state,
                events: recovered_events,
            });
        }
        self.run_from_state(
            spec,
            checkpoint.state,
            CommitCursor {
                last_sequence: checkpoint.last_sequence,
                applied_commit_ids: checkpoint.applied_commit_ids,
            },
            true,
        )
    }

    fn run_from_state(
        &self,
        spec: &RunSpec,
        mut state: RunState,
        mut cursor: CommitCursor,
        resumed: bool,
    ) -> Result<RunReport, KernelError> {
        let mut events = Vec::new();
        let mut event_bus = InMemoryEventBus::new();
        let spec_digest = spec.digest();

        if !resumed {
            self.push_event(
                &mut events,
                &mut event_bus,
                &mut cursor,
                &spec_digest,
                Event::new(&spec.run_id, None, EventKind::RunCreated, &spec.name),
            )?;
        }
        state.status = RunStatus::Running;
        self.push_event(
            &mut events,
            &mut event_bus,
            &mut cursor,
            &spec_digest,
            Event::new(
                &spec.run_id,
                None,
                EventKind::RunStarted,
                if resumed {
                    "kernel_resumed".to_string()
                } else {
                    "kernel_started".to_string()
                },
            ),
        )?;
        self.checkpoint(&state, &spec_digest, &cursor)?;

        while let Some(original_step) = self.scheduler.next(spec, state.next_step_index) {
            if state.next_step_index >= spec.budget.max_steps {
                state.status = RunStatus::Failed;
                let detail = format!("budget_exceeded:{}", spec.budget.max_steps);
                self.push_event(
                    &mut events,
                    &mut event_bus,
                    &mut cursor,
                    &spec_digest,
                    Event::new(
                        &spec.run_id,
                        Some(original_step.id.clone()),
                        EventKind::RunFailed,
                        detail.clone(),
                    ),
                )?;
                self.checkpoint(&state, &spec_digest, &cursor)?;
                return Err(KernelError::BudgetExceeded(detail));
            }

            self.push_event(
                &mut events,
                &mut event_bus,
                &mut cursor,
                &spec_digest,
                Event::new(
                    &spec.run_id,
                    Some(original_step.id.clone()),
                    EventKind::StepProposed,
                    &original_step.title,
                ),
            )?;

            let step = match self.shell.before_step(spec, &state, original_step) {
                ShellDecision::Allow => {
                    self.push_event(
                        &mut events,
                        &mut event_bus,
                        &mut cursor,
                        &spec_digest,
                        Event::new(
                            &spec.run_id,
                            Some(original_step.id.clone()),
                            EventKind::ShellDecision,
                            "allow",
                        ),
                    )?;
                    original_step.clone()
                }
                ShellDecision::Rewrite { new_step, reason } => {
                    self.push_event(
                        &mut events,
                        &mut event_bus,
                        &mut cursor,
                        &spec_digest,
                        Event::new(
                            &spec.run_id,
                            Some(original_step.id.clone()),
                            EventKind::ShellDecision,
                            format!("rewrite:{reason}"),
                        ),
                    )?;
                    new_step
                }
                ShellDecision::Deny { reason } => {
                    state.denied_actions.push(original_step.id.clone());
                    self.push_event(
                        &mut events,
                        &mut event_bus,
                        &mut cursor,
                        &spec_digest,
                        Event::new(
                            &spec.run_id,
                            Some(original_step.id.clone()),
                            EventKind::ShellDecision,
                            format!("deny:{reason}"),
                        ),
                    )?;
                    state.next_step_index += 1;
                    self.checkpoint(&state, &spec_digest, &cursor)?;
                    continue;
                }
            };

            self.push_event(
                &mut events,
                &mut event_bus,
                &mut cursor,
                &spec_digest,
                Event::new(
                    &spec.run_id,
                    Some(step.id.clone()),
                    EventKind::StepStarted,
                    &step.title,
                ),
            )?;

            let proposal =
                self.execute_step(spec, &state, &step, &mut events, &mut cursor, &spec_digest)?;
            self.push_event(
                &mut events,
                &mut event_bus,
                &mut cursor,
                &spec_digest,
                Event::new(
                    &spec.run_id,
                    Some(step.id.clone()),
                    EventKind::StepCompleted,
                    &proposal.summary,
                ),
            )?;
            self.push_event(
                &mut events,
                &mut event_bus,
                &mut cursor,
                &spec_digest,
                Event::new(
                    &spec.run_id,
                    Some(step.id.clone()),
                    EventKind::EffectProposed,
                    &proposal.summary,
                ),
            )?;
            let effect: Effect = proposal.into();
            let commit_id = format!("{}:{}:{}", spec.run_id, step.id, spec_digest);
            let mut committed_event = Event::new(
                &spec.run_id,
                Some(step.id.clone()),
                EventKind::EffectCommitted,
                &effect.summary,
            );
            committed_event.commit_id = Some(commit_id.clone());
            committed_event.effect = Some(effect.clone());
            self.push_event(
                &mut events,
                &mut event_bus,
                &mut cursor,
                &spec_digest,
                committed_event,
            )?;
            if cursor.applied_commit_ids.insert(commit_id) {
                apply_effect(&mut state, effect);
            }

            state.next_step_index += 1;
            self.checkpoint(&state, &spec_digest, &cursor)?;
        }

        self.push_event(
            &mut events,
            &mut event_bus,
            &mut cursor,
            &spec_digest,
            Event::new(&spec.run_id, None, EventKind::RunCompleted, "ok"),
        )?;
        state.status = RunStatus::Completed;
        self.checkpoint(&state, &spec_digest, &cursor)?;

        let _ = event_bus.snapshot();

        Ok(RunReport { state, events })
    }

    fn execute_step(
        &self,
        spec: &RunSpec,
        state: &RunState,
        step: &Step,
        events: &mut Vec<Event>,
        cursor: &mut CommitCursor,
        spec_digest: &str,
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
                            cursor,
                            spec_digest,
                            Event::new(
                                &spec.run_id,
                                Some(step.id.clone()),
                                EventKind::CapabilityDenied,
                                detail.clone(),
                            ),
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
                        cursor,
                        spec_digest,
                        Event::new(
                            &spec.run_id,
                            Some(step.id.clone()),
                            EventKind::CapabilityDenied,
                            detail.clone(),
                        ),
                    )?;
                    return Err(KernelError::Denied(detail));
                }
                self.push_event(
                    events,
                    &mut InMemoryEventBus::new(),
                    cursor,
                    spec_digest,
                    Event::new(
                        &spec.run_id,
                        Some(step.id.clone()),
                        EventKind::ChildRunCreated,
                        &child.run.run_id,
                    ),
                )?;
                let child_result = self
                    .subrun_transport
                    .execute(child, &|child_spec| self.run(child_spec))?;
                self.push_event(
                    events,
                    &mut InMemoryEventBus::new(),
                    cursor,
                    spec_digest,
                    Event::new(
                        &spec.run_id,
                        Some(step.id.clone()),
                        EventKind::ChildRunCompleted,
                        format!(
                            "{}:{:?}:{}",
                            child_result.run_id, child_result.status, child_result.events_digest
                        ),
                    ),
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
        cursor: &mut CommitCursor,
        spec_digest: &str,
        mut event: Event,
    ) -> Result<(), KernelError> {
        let next_sequence = cursor.last_sequence + 1;
        event.sequence = next_sequence;
        event.timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| KernelError::EventLog(err.to_string()))?
            .as_millis() as u64;
        event.spec_digest = spec_digest.to_string();
        event.causation_id = (cursor.last_sequence > 0)
            .then(|| format!("{}:{}", event.run_id, cursor.last_sequence));
        if let Some(event_log) = &self.event_log {
            event_log.append(&event).map_err(KernelError::EventLog)?;
        }
        cursor.last_sequence = next_sequence;
        event_bus.publish(event.clone());
        events.push(event);
        Ok(())
    }

    fn checkpoint(
        &self,
        state: &RunState,
        spec_digest: &str,
        cursor: &CommitCursor,
    ) -> Result<(), KernelError> {
        self.run_store
            .put(RunStoreRecord {
                checkpoint_version: 1,
                run_id: state.run_id.clone(),
                spec_digest: spec_digest.to_string(),
                last_sequence: cursor.last_sequence,
                applied_commit_ids: cursor.applied_commit_ids.clone(),
                state: state.clone(),
            })
            .map_err(KernelError::RunStore)
    }

    fn recover_from_journal(
        &self,
        spec: &RunSpec,
        checkpoint: &mut RunStoreRecord,
    ) -> Result<Vec<Event>, KernelError> {
        let Some(event_log) = &self.event_log else {
            return Ok(Vec::new());
        };
        let events = event_log
            .load_after(&spec.run_id, checkpoint.last_sequence)
            .map_err(KernelError::EventLog)?;
        for event in &events {
            if event.schema_version != 1 {
                return Err(KernelError::EventLog(format!(
                    "unsupported_event_version:{}",
                    event.schema_version
                )));
            }
            if event.spec_digest != checkpoint.spec_digest {
                return Err(KernelError::EventLog(format!(
                    "event_spec_digest_mismatch:{}:{}",
                    event.spec_digest, checkpoint.spec_digest
                )));
            }
            if event.sequence != checkpoint.last_sequence + 1 {
                return Err(KernelError::EventLog(format!(
                    "event_sequence_gap:{}:{}",
                    checkpoint.last_sequence, event.sequence
                )));
            }
            match event.kind {
                EventKind::EffectCommitted => {
                    let commit_id = event.commit_id.as_ref().ok_or_else(|| {
                        KernelError::EventLog("committed_effect_missing_commit_id".to_string())
                    })?;
                    if checkpoint.applied_commit_ids.insert(commit_id.clone()) {
                        let effect = event.effect.clone().ok_or_else(|| {
                            KernelError::EventLog("committed_effect_missing_payload".to_string())
                        })?;
                        apply_effect(&mut checkpoint.state, effect);
                        let step_id = event.step_id.as_ref().ok_or_else(|| {
                            KernelError::EventLog("committed_effect_missing_step_id".to_string())
                        })?;
                        let step_index = spec
                            .steps
                            .iter()
                            .position(|step| &step.id == step_id)
                            .ok_or_else(|| {
                                KernelError::EventLog(format!(
                                    "committed_effect_unknown_step:{step_id}"
                                ))
                            })?;
                        checkpoint.state.next_step_index =
                            checkpoint.state.next_step_index.max(step_index + 1);
                    }
                }
                EventKind::RunCompleted => checkpoint.state.status = RunStatus::Completed,
                EventKind::RunFailed => checkpoint.state.status = RunStatus::Failed,
                _ => {}
            }
            checkpoint.last_sequence = event.sequence;
        }
        Ok(events)
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
