use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use axiom_spec::{Event, EventKind};

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
        file.write_all(line.as_bytes()).map_err(|err| err.to_string())
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
}

fn event_kind_name(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::RunCreated => "RunCreated",
        EventKind::RunStarted => "RunStarted",
        EventKind::StepProposed => "StepProposed",
        EventKind::ShellDecision => "ShellDecision",
        EventKind::StepStarted => "StepStarted",
        EventKind::StepCompleted => "StepCompleted",
        EventKind::EffectApplied => "EffectApplied",
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
