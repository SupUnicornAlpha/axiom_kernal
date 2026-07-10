use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use axiom_spec::{Event, EventKind, RunState, EVENT_SCHEMA_VERSION, RUN_SPEC_SCHEMA_VERSION};
use serde::Deserialize;

use crate::RunStoreRecord;

pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationStatus {
    Current,
    Migrated { from_version: u32 },
}

#[derive(Clone, Debug)]
pub struct MigrationOutcome<T> {
    pub value: T,
    pub status: MigrationStatus,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct EventMigrationContext {
    pub spec_digest: String,
    pub writer_epoch: u64,
}

#[derive(Clone, Debug)]
pub struct CheckpointMigrationContext {
    pub spec_digest: String,
    pub last_sequence: u64,
    pub writer_epoch: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    pub total_records: usize,
    pub migrated_records: usize,
    pub current_records: usize,
    pub warnings: Vec<String>,
}

#[derive(Deserialize)]
struct LegacyEventV0 {
    run_id: String,
    step_id: Option<String>,
    kind: String,
    detail: String,
}

#[derive(Deserialize)]
struct LegacyCheckpointV0 {
    run_id: String,
    state: RunState,
}

pub fn migrate_event_json(
    line: &str,
    context: &EventMigrationContext,
    sequence: u64,
) -> Result<MigrationOutcome<Event>, String> {
    let value: serde_json::Value = serde_json::from_str(line).map_err(|err| err.to_string())?;
    let version = value
        .get("schema_version")
        .and_then(|value| value.as_u64())
        .unwrap_or(0) as u32;
    match version {
        EVENT_SCHEMA_VERSION => {
            let event: Event = serde_json::from_value(value).map_err(|err| err.to_string())?;
            Ok(MigrationOutcome {
                value: event,
                status: MigrationStatus::Current,
                warnings: Vec::new(),
            })
        }
        0 => {
            let legacy: LegacyEventV0 =
                serde_json::from_value(value).map_err(|err| err.to_string())?;
            let (kind, warning) = migrate_event_kind(&legacy.kind)?;
            let mut event = Event::new(legacy.run_id, legacy.step_id, kind, legacy.detail);
            event.sequence = sequence;
            event.timestamp_ms = 0;
            event.spec_digest = context.spec_digest.clone();
            event.writer_epoch = context.writer_epoch;
            event.causation_id =
                (sequence > 1).then(|| format!("{}:{}", event.run_id, sequence.saturating_sub(1)));
            Ok(MigrationOutcome {
                value: event,
                status: MigrationStatus::Migrated { from_version: 0 },
                warnings: warning.into_iter().collect(),
            })
        }
        future => Err(format!(
            "unsupported_event_schema_version:{future}:current={EVENT_SCHEMA_VERSION}"
        )),
    }
}

pub fn migrate_checkpoint_json(
    json: &str,
    context: &CheckpointMigrationContext,
) -> Result<MigrationOutcome<RunStoreRecord>, String> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|err| err.to_string())?;
    let version = value
        .get("checkpoint_version")
        .and_then(|value| value.as_u64())
        .unwrap_or(0) as u32;
    match version {
        CHECKPOINT_SCHEMA_VERSION => {
            let record: RunStoreRecord =
                serde_json::from_value(value).map_err(|err| err.to_string())?;
            Ok(MigrationOutcome {
                value: record,
                status: MigrationStatus::Current,
                warnings: Vec::new(),
            })
        }
        0 => {
            let legacy: LegacyCheckpointV0 =
                serde_json::from_value(value).map_err(|err| err.to_string())?;
            Ok(MigrationOutcome {
                value: RunStoreRecord {
                    checkpoint_version: CHECKPOINT_SCHEMA_VERSION,
                    run_id: legacy.run_id,
                    spec_digest: context.spec_digest.clone(),
                    last_sequence: context.last_sequence,
                    writer_epoch: context.writer_epoch,
                    applied_commit_ids: BTreeSet::new(),
                    state: legacy.state,
                },
                status: MigrationStatus::Migrated { from_version: 0 },
                warnings: vec![
                    "legacy_checkpoint_has_no_commit_ids; migrate only at a trusted journal boundary"
                        .to_string(),
                ],
            })
        }
        future => Err(format!(
            "unsupported_checkpoint_schema_version:{future}:current={CHECKPOINT_SCHEMA_VERSION}"
        )),
    }
}

pub fn migrate_event_log(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    context: &EventMigrationContext,
) -> Result<MigrationReport, String> {
    let source = File::open(source).map_err(|err| err.to_string())?;
    let reader = BufReader::new(source);
    let mut target = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(target)
        .map_err(|err| err.to_string())?;
    let mut report = MigrationReport::default();
    for line in reader.lines() {
        let line = line.map_err(|err| err.to_string())?;
        let sequence = report.total_records as u64 + 1;
        let outcome = migrate_event_json(&line, context, sequence)?;
        report.total_records += 1;
        match outcome.status {
            MigrationStatus::Current => report.current_records += 1,
            MigrationStatus::Migrated { .. } => report.migrated_records += 1,
        }
        report.warnings.extend(outcome.warnings);
        serde_json::to_writer(&mut target, &outcome.value).map_err(|err| err.to_string())?;
        target.write_all(b"\n").map_err(|err| err.to_string())?;
    }
    target.flush().map_err(|err| err.to_string())?;
    target.sync_all().map_err(|err| err.to_string())?;
    Ok(report)
}

pub fn migrate_checkpoint_file(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    context: &CheckpointMigrationContext,
) -> Result<MigrationOutcome<RunStoreRecord>, String> {
    let json = std::fs::read_to_string(source).map_err(|err| err.to_string())?;
    let outcome = migrate_checkpoint_json(&json, context)?;
    let mut target = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(target)
        .map_err(|err| err.to_string())?;
    serde_json::to_writer(&mut target, &outcome.value).map_err(|err| err.to_string())?;
    target.flush().map_err(|err| err.to_string())?;
    target.sync_all().map_err(|err| err.to_string())?;
    Ok(outcome)
}

fn migrate_event_kind(kind: &str) -> Result<(EventKind, Option<String>), String> {
    let migrated = match kind {
        "RunCreated" => (EventKind::RunCreated, None),
        "RunStarted" => (EventKind::RunStarted, None),
        "StepProposed" => (EventKind::StepProposed, None),
        "ShellDecision" => (EventKind::ShellDecision, None),
        "StepStarted" => (EventKind::StepStarted, None),
        "StepCompleted" => (EventKind::StepCompleted, None),
        "EffectApplied" => (
            EventKind::EffectCommitted,
            Some(
                "legacy EffectApplied has no payload; recovery requires a checkpoint at or after this event"
                    .to_string(),
            ),
        ),
        "CapabilityDenied" => (EventKind::CapabilityDenied, None),
        "ChildRunCreated" => (EventKind::ChildRunCreated, None),
        "ChildRunCompleted" => (EventKind::ChildRunCompleted, None),
        "RunCompleted" => (EventKind::RunCompleted, None),
        "RunFailed" => (EventKind::RunFailed, None),
        unknown => return Err(format!("unknown_legacy_event_kind:{unknown}")),
    };
    Ok(migrated)
}

pub fn validate_run_spec_version(schema_version: u32) -> Result<(), String> {
    if schema_version == RUN_SPEC_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(format!(
            "unsupported_runspec_schema_version:{schema_version}:current={RUN_SPEC_SCHEMA_VERSION}"
        ))
    }
}
