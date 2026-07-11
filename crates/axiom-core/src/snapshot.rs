use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::RunStoreRecord;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub run_id: String,
    pub sequence: u64,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotRetentionReport {
    pub run_id: String,
    pub retained_sequences: Vec<u64>,
    pub deleted_sequences: Vec<u64>,
}

pub trait SnapshotArchive: Send + Sync {
    fn archive(&self, record: &RunStoreRecord) -> Result<SnapshotMetadata, String>;
    fn load(&self, run_id: &str, sequence: u64) -> Result<Option<RunStoreRecord>, String>;
    fn list(&self, run_id: &str) -> Result<Vec<SnapshotMetadata>, String>;
    fn retain_latest(&self, run_id: &str, keep: usize) -> Result<SnapshotRetentionReport, String>;
}

#[derive(Clone, Debug)]
pub struct FileSnapshotArchive {
    root: PathBuf,
}

impl FileSnapshotArchive {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn run_root(&self, run_id: &str) -> PathBuf {
        self.root.join(hex_id(run_id))
    }

    fn snapshot_path(&self, run_id: &str, sequence: u64) -> PathBuf {
        self.run_root(run_id).join(format!("{sequence:020}.json"))
    }
}

impl SnapshotArchive for FileSnapshotArchive {
    fn archive(&self, record: &RunStoreRecord) -> Result<SnapshotMetadata, String> {
        let run_root = self.run_root(&record.run_id);
        fs::create_dir_all(&run_root).map_err(|err| err.to_string())?;
        let target = self.snapshot_path(&record.run_id, record.last_sequence);
        let temporary = target.with_extension("json.tmp");
        let bytes = serde_json::to_vec(record).map_err(|err| err.to_string())?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|err| err.to_string())?;
        file.write_all(&bytes).map_err(|err| err.to_string())?;
        file.flush().map_err(|err| err.to_string())?;
        file.sync_all().map_err(|err| err.to_string())?;
        fs::rename(&temporary, &target).map_err(|err| err.to_string())?;
        File::open(&run_root)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| err.to_string())?;
        Ok(SnapshotMetadata {
            run_id: record.run_id.clone(),
            sequence: record.last_sequence,
            path: target,
        })
    }

    fn load(&self, run_id: &str, sequence: u64) -> Result<Option<RunStoreRecord>, String> {
        let path = self.snapshot_path(run_id, sequence);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).map_err(|err| err.to_string())?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| err.to_string())
    }

    fn list(&self, run_id: &str) -> Result<Vec<SnapshotMetadata>, String> {
        let run_root = self.run_root(run_id);
        if !run_root.exists() {
            return Ok(Vec::new());
        }
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(&run_root).map_err(|err| err.to_string())? {
            let entry = entry.map_err(|err| err.to_string())?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            let sequence = stem.parse::<u64>().map_err(|err| err.to_string())?;
            snapshots.push(SnapshotMetadata {
                run_id: run_id.to_string(),
                sequence,
                path,
            });
        }
        snapshots.sort_by_key(|snapshot| snapshot.sequence);
        Ok(snapshots)
    }

    fn retain_latest(&self, run_id: &str, keep: usize) -> Result<SnapshotRetentionReport, String> {
        let snapshots = self.list(run_id)?;
        let delete_count = snapshots.len().saturating_sub(keep);
        let mut report = SnapshotRetentionReport {
            run_id: run_id.to_string(),
            ..SnapshotRetentionReport::default()
        };
        for snapshot in snapshots.iter().take(delete_count) {
            fs::remove_file(&snapshot.path).map_err(|err| err.to_string())?;
            report.deleted_sequences.push(snapshot.sequence);
        }
        report.retained_sequences = snapshots
            .iter()
            .skip(delete_count)
            .map(|snapshot| snapshot.sequence)
            .collect();
        let run_root = self.run_root(run_id);
        if run_root.exists() {
            File::open(run_root)
                .and_then(|directory| directory.sync_all())
                .map_err(|err| err.to_string())?;
        }
        Ok(report)
    }
}

fn hex_id(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
