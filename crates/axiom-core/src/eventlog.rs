use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use axiom_spec::{Event, EventKind};

pub trait EventJournal: Send + Sync {
    fn append(&self, event: &Event) -> Result<(), String>;
}

#[derive(Clone, Debug)]
pub struct JsonlEventLog {
    path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaySummary {
    pub total_events: usize,
    pub completed_runs: usize,
    pub failed_runs: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationSummary {
    pub valid_lines: usize,
    pub invalid_lines: usize,
}

impl JsonlEventLog {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, event: &Event) -> Result<(), String> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|err| err.to_string())?;
        let line = format!(
            "{{\"run_id\":\"{}\",\"step_id\":{},\"kind\":\"{}\",\"detail\":\"{}\"}}\n",
            escape(&event.run_id),
            event
                .step_id
                .as_ref()
                .map(|value| format!("\"{}\"", escape(value)))
                .unwrap_or_else(|| "null".to_string()),
            event_kind_name(&event.kind),
            escape(&event.detail)
        );
        file.write_all(line.as_bytes())
            .and_then(|_| file.flush())
            .and_then(|_| file.sync_data())
            .map_err(|err| err.to_string())
    }

    pub fn replay_summary(&self) -> Result<ReplaySummary, String> {
        if !self.path.exists() {
            return Ok(ReplaySummary {
                total_events: 0,
                completed_runs: 0,
                failed_runs: 0,
            });
        }

        let file = File::open(&self.path).map_err(|err| err.to_string())?;
        let reader = BufReader::new(file);

        let mut total_events = 0;
        let mut completed_runs = 0;
        let mut failed_runs = 0;

        for line in reader.lines() {
            let line = line.map_err(|err| err.to_string())?;
            total_events += 1;
            if line.contains("\"kind\":\"RunCompleted\"") {
                completed_runs += 1;
            }
            if line.contains("\"kind\":\"RunFailed\"") {
                failed_runs += 1;
            }
        }

        Ok(ReplaySummary {
            total_events,
            completed_runs,
            failed_runs,
        })
    }

    pub fn validate_lines(&self) -> Result<ValidationSummary, String> {
        if !self.path.exists() {
            return Ok(ValidationSummary {
                valid_lines: 0,
                invalid_lines: 0,
            });
        }

        let file = File::open(&self.path).map_err(|err| err.to_string())?;
        let reader = BufReader::new(file);
        let mut valid_lines = 0;
        let mut invalid_lines = 0;

        for line in reader.lines() {
            let line = line.map_err(|err| err.to_string())?;
            if line.starts_with("{\"run_id\":\"")
                && line.contains("\"kind\":\"")
                && line.ends_with('}')
            {
                valid_lines += 1;
            } else {
                invalid_lines += 1;
            }
        }

        Ok(ValidationSummary {
            valid_lines,
            invalid_lines,
        })
    }
}

impl EventJournal for JsonlEventLog {
    fn append(&self, event: &Event) -> Result<(), String> {
        JsonlEventLog::append(self, event)
    }
}

fn event_kind_name(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::RunCreated => "RunCreated",
        EventKind::RunStarted => "RunStarted",
        EventKind::StepProposed => "StepProposed",
        EventKind::ShellDecision => "ShellDecision",
        EventKind::StepStarted => "StepStarted",
        EventKind::StepCompleted => "StepCompleted",
        EventKind::EffectProposed => "EffectProposed",
        EventKind::EffectCommitted => "EffectCommitted",
        EventKind::CapabilityDenied => "CapabilityDenied",
        EventKind::ChildRunCreated => "ChildRunCreated",
        EventKind::ChildRunCompleted => "ChildRunCompleted",
        EventKind::RunCompleted => "RunCompleted",
        EventKind::RunFailed => "RunFailed",
    }
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
