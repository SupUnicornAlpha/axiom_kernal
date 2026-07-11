use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use axiom_spec::{Event, EventKind};

pub trait EventJournal: Send + Sync {
    fn append(&self, event: &Event) -> Result<(), String>;
    fn load_after(&self, run_id: &str, sequence: u64) -> Result<Vec<Event>, String>;
}

#[derive(Clone, Debug)]
pub struct JsonlEventLog {
    path: PathBuf,
}

#[derive(Debug)]
pub struct GenerationEventLog {
    root: PathBuf,
    switch_lock: Mutex<()>,
}

#[derive(Clone, Debug)]
pub struct GenerationReader {
    pub generation: u64,
    journal: JsonlEventLog,
}

impl GenerationEventLog {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let this = Self {
            root: root.into(),
            switch_lock: Mutex::new(()),
        };
        fs::create_dir_all(&this.root).map_err(|error| error.to_string())?;
        if !this.manifest_path().exists() {
            this.write_manifest(0)?;
        }
        Ok(this)
    }

    pub fn reader(&self) -> Result<GenerationReader, String> {
        let generation = self.current_generation()?;
        Ok(GenerationReader {
            generation,
            journal: JsonlEventLog::new(self.generation_path(generation)),
        })
    }

    pub fn compact_online(
        &self,
        boundaries: BTreeMap<String, u64>,
    ) -> Result<JournalCompactionReport, String> {
        let _guard = self
            .switch_lock
            .lock()
            .map_err(|_| "generation_switch_lock_poisoned".to_string())?;
        let current = self.current_generation()?;
        let next = current + 1;
        let report = JsonlEventLog::new(self.generation_path(current))
            .compact_to(self.generation_path(next), boundaries)?;
        self.write_manifest(next)?;
        Ok(report)
    }

    fn current_generation(&self) -> Result<u64, String> {
        fs::read_to_string(self.manifest_path())
            .map_err(|error| error.to_string())?
            .trim()
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join("CURRENT")
    }
    fn generation_path(&self, generation: u64) -> PathBuf {
        self.root.join(format!("events-{generation}.jsonl"))
    }
    fn write_manifest(&self, generation: u64) -> Result<(), String> {
        let temporary = self.root.join("CURRENT.tmp");
        fs::write(&temporary, generation.to_string()).map_err(|error| error.to_string())?;
        fs::rename(temporary, self.manifest_path()).map_err(|error| error.to_string())
    }
}

impl GenerationReader {
    pub fn load_after(&self, run_id: &str, sequence: u64) -> Result<Vec<Event>, String> {
        self.journal.load_after(run_id, sequence)
    }
}

impl EventJournal for GenerationEventLog {
    fn append(&self, event: &Event) -> Result<(), String> {
        let _guard = self
            .switch_lock
            .lock()
            .map_err(|_| "generation_switch_lock_poisoned".to_string())?;
        JsonlEventLog::new(self.generation_path(self.current_generation()?)).append(event)
    }

    fn load_after(&self, run_id: &str, sequence: u64) -> Result<Vec<Event>, String> {
        self.reader()?.load_after(run_id, sequence)
    }
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalIntegrityReport {
    pub total_lines: usize,
    pub valid_events: usize,
    pub corrupt_lines: Vec<usize>,
    pub sequence_anomalies: Vec<String>,
    pub duplicate_commit_ids: Vec<String>,
}

impl JournalIntegrityReport {
    pub fn is_clean(&self) -> bool {
        self.corrupt_lines.is_empty()
            && self.sequence_anomalies.is_empty()
            && self.duplicate_commit_ids.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalRepairReport {
    pub source_lines: usize,
    pub repaired_events: usize,
    pub quarantined_lines: Vec<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalCompactionReport {
    pub source_events: usize,
    pub retained_events: usize,
    pub dropped_events: usize,
    pub snapshot_boundaries: BTreeMap<String, u64>,
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

    pub fn scan_integrity(&self) -> Result<JournalIntegrityReport, String> {
        if !self.path.exists() {
            return Ok(JournalIntegrityReport::default());
        }
        let file = File::open(&self.path).map_err(|err| err.to_string())?;
        let reader = BufReader::new(file);
        let mut report = JournalIntegrityReport::default();
        let mut last_sequence_by_run = BTreeMap::<String, u64>::new();
        let mut commit_ids = BTreeSet::<String>::new();
        for (line_index, line) in reader.lines().enumerate() {
            let line_number = line_index + 1;
            let line = line.map_err(|err| err.to_string())?;
            report.total_lines += 1;
            let Ok(event) = serde_json::from_str::<Event>(&line) else {
                report.corrupt_lines.push(line_number);
                continue;
            };
            report.valid_events += 1;
            if let Some(previous) = last_sequence_by_run.get(&event.run_id) {
                if event.sequence != previous + 1 {
                    report.sequence_anomalies.push(format!(
                        "{}:{}:{}:{}",
                        event.run_id, line_number, previous, event.sequence
                    ));
                }
            }
            last_sequence_by_run.insert(event.run_id.clone(), event.sequence);
            if let Some(commit_id) = event.commit_id {
                if !commit_ids.insert(commit_id.clone()) {
                    report.duplicate_commit_ids.push(commit_id);
                }
            }
        }
        Ok(report)
    }

    pub fn repair_to(
        &self,
        target: impl AsRef<Path>,
        quarantine: impl AsRef<Path>,
    ) -> Result<JournalRepairReport, String> {
        let integrity = self.scan_integrity()?;
        if !integrity.sequence_anomalies.is_empty() || !integrity.duplicate_commit_ids.is_empty() {
            return Err("journal_semantic_corruption_requires_manual_reconciliation".to_string());
        }
        let file = File::open(&self.path).map_err(|err| err.to_string())?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();
        let mut quarantined = Vec::new();
        for (line_index, line) in reader.lines().enumerate() {
            let line_number = line_index + 1;
            let line = line.map_err(|err| err.to_string())?;
            match serde_json::from_str::<Event>(&line) {
                Ok(event) => events.push(event),
                Err(_) => quarantined.push((line_number, line)),
            }
        }
        write_events_atomic(target.as_ref(), &events)?;
        write_quarantine(quarantine.as_ref(), &quarantined)?;
        Ok(JournalRepairReport {
            source_lines: integrity.total_lines,
            repaired_events: events.len(),
            quarantined_lines: quarantined
                .into_iter()
                .map(|(line_number, _)| line_number)
                .collect(),
        })
    }

    pub fn compact_to(
        &self,
        target: impl AsRef<Path>,
        snapshot_boundaries: BTreeMap<String, u64>,
    ) -> Result<JournalCompactionReport, String> {
        let integrity = self.scan_integrity()?;
        if !integrity.is_clean() {
            return Err("journal_must_be_clean_before_compaction".to_string());
        }
        let file = File::open(&self.path).map_err(|err| err.to_string())?;
        let reader = BufReader::new(file);
        let mut retained = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|err| err.to_string())?;
            let event: Event = serde_json::from_str(&line).map_err(|err| err.to_string())?;
            let boundary = snapshot_boundaries.get(&event.run_id).copied().unwrap_or(0);
            if event.sequence > boundary {
                retained.push(event);
            }
        }
        write_events_atomic(target.as_ref(), &retained)?;
        Ok(JournalCompactionReport {
            source_events: integrity.valid_events,
            retained_events: retained.len(),
            dropped_events: integrity.valid_events.saturating_sub(retained.len()),
            snapshot_boundaries,
        })
    }
}

fn write_events_atomic(target: &Path, events: &[Event]) -> Result<(), String> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let temporary = target.with_extension("jsonl.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|err| err.to_string())?;
    for event in events {
        serde_json::to_writer(&mut file, event).map_err(|err| err.to_string())?;
        file.write_all(b"\n").map_err(|err| err.to_string())?;
    }
    file.flush().map_err(|err| err.to_string())?;
    file.sync_all().map_err(|err| err.to_string())?;
    fs::rename(temporary, target).map_err(|err| err.to_string())
}

fn write_quarantine(target: &Path, lines: &[(usize, String)]) -> Result<(), String> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(target)
        .map_err(|err| err.to_string())?;
    for (line_number, line) in lines {
        writeln!(file, "{line_number}\t{line}").map_err(|err| err.to_string())?;
    }
    file.flush().map_err(|err| err.to_string())?;
    file.sync_all().map_err(|err| err.to_string())
}

impl EventJournal for JsonlEventLog {
    fn append(&self, event: &Event) -> Result<(), String> {
        JsonlEventLog::append(self, event)
    }

    fn load_after(&self, run_id: &str, sequence: u64) -> Result<Vec<Event>, String> {
        JsonlEventLog::load_after(self, run_id, sequence)
    }
}

#[cfg(test)]
mod generation_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn readers_survive_generation_switch_under_concurrent_appends() {
        let root = std::env::temp_dir().join(format!("axiom-generation-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let journal = Arc::new(GenerationEventLog::new(&root).unwrap());
        for sequence in 1..=20 {
            journal.append(&event(sequence)).unwrap();
        }
        let old_reader = journal.reader().unwrap();
        let writer = Arc::clone(&journal);
        let handle = thread::spawn(move || {
            for sequence in 21..=40 {
                writer.append(&event(sequence)).unwrap();
            }
        });
        handle.join().unwrap();
        journal
            .compact_online(BTreeMap::from([("generation-run".to_string(), 20)]))
            .unwrap();
        assert_eq!(
            old_reader.load_after("generation-run", 0).unwrap().len(),
            40
        );
        let new_reader = journal.reader().unwrap();
        assert!(new_reader.generation > old_reader.generation);
        assert_eq!(
            new_reader.load_after("generation-run", 0).unwrap().len(),
            20
        );
    }

    fn event(sequence: u64) -> Event {
        let mut event = Event::new("generation-run", None, EventKind::RunStarted, "stress");
        event.sequence = sequence;
        event.timestamp_ms = sequence;
        event.spec_digest = "0".repeat(64);
        event.writer_epoch = 1;
        event
    }
}
