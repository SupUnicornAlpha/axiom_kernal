use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use axiom_spec::{Event, EventKind};

pub trait EventJournal: Send + Sync {
    fn append(&self, event: &Event) -> Result<(), String>;
    fn load_after(&self, run_id: &str, sequence: u64) -> Result<Vec<Event>, String>;
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
        let mut line = serde_json::to_string(event).map_err(|err| err.to_string())?;
        line.push('\n');
        file.write_all(line.as_bytes())
            .and_then(|_| file.flush())
            .and_then(|_| file.sync_data())
            .map_err(|err| err.to_string())
    }

    pub fn load_after(&self, run_id: &str, sequence: u64) -> Result<Vec<Event>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&self.path).map_err(|err| err.to_string())?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|err| err.to_string())?;
            let event: Event = serde_json::from_str(&line).map_err(|err| err.to_string())?;
            if event.run_id == run_id && event.sequence > sequence {
                events.push(event);
            }
        }
        events.sort_by_key(|event| event.sequence);
        Ok(events)
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
            let event: Event = serde_json::from_str(&line).map_err(|err| err.to_string())?;
            match event.kind {
                EventKind::RunCompleted => completed_runs += 1,
                EventKind::RunFailed => failed_runs += 1,
                _ => {}
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
            if serde_json::from_str::<Event>(&line).is_ok() {
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

    fn load_after(&self, run_id: &str, sequence: u64) -> Result<Vec<Event>, String> {
        JsonlEventLog::load_after(self, run_id, sequence)
    }
}
