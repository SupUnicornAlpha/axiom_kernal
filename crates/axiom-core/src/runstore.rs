use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use axiom_spec::RunState;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunStoreRecord {
    pub checkpoint_version: u32,
    pub run_id: String,
    pub spec_digest: String,
    pub last_sequence: u64,
    pub writer_epoch: u64,
    pub applied_commit_ids: BTreeSet<String>,
    pub state: RunState,
}

pub trait RunStore: Send + Sync {
    fn put(&self, record: RunStoreRecord) -> Result<(), String>;
    fn get(&self, run_id: &str) -> Result<Option<RunStoreRecord>, String>;
    fn list_run_ids(&self) -> Result<Vec<String>, String>;
}

#[derive(Clone, Debug)]
pub struct FileRunStore {
    root: PathBuf,
}

impl FileRunStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn record_path(&self, run_id: &str) -> PathBuf {
        self.root.join(format!("{}.json", hex_id(run_id)))
    }
}

impl RunStore for FileRunStore {
    fn put(&self, record: RunStoreRecord) -> Result<(), String> {
        fs::create_dir_all(&self.root).map_err(|err| err.to_string())?;
        let target = self.record_path(&record.run_id);
        let temporary = target.with_extension("json.tmp");
        let bytes = serde_json::to_vec(&record).map_err(|err| err.to_string())?;
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
        File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| err.to_string())
    }

    fn get(&self, run_id: &str) -> Result<Option<RunStoreRecord>, String> {
        let path = self.record_path(run_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).map_err(|err| err.to_string())?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| err.to_string())
    }

    fn list_run_ids(&self) -> Result<Vec<String>, String> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut run_ids = Vec::new();
        for entry in fs::read_dir(&self.root).map_err(|err| err.to_string())? {
            let entry = entry.map_err(|err| err.to_string())?;
            if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(entry.path()).map_err(|err| err.to_string())?;
            let record: RunStoreRecord =
                serde_json::from_slice(&bytes).map_err(|err| err.to_string())?;
            run_ids.push(record.run_id);
        }
        run_ids.sort();
        Ok(run_ids)
    }
}

fn hex_id(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Default, Clone, Debug)]
pub struct MemoryRunStore {
    records: Arc<RwLock<BTreeMap<String, RunStoreRecord>>>,
}

impl MemoryRunStore {
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl RunStore for MemoryRunStore {
    fn put(&self, record: RunStoreRecord) -> Result<(), String> {
        self.records
            .write()
            .map_err(|_| "run_store_write_lock_poisoned".to_string())?
            .insert(record.run_id.clone(), record);
        Ok(())
    }

    fn get(&self, run_id: &str) -> Result<Option<RunStoreRecord>, String> {
        Ok(self
            .records
            .read()
            .map_err(|_| "run_store_read_lock_poisoned".to_string())?
            .get(run_id)
            .cloned())
    }

    fn list_run_ids(&self) -> Result<Vec<String>, String> {
        Ok(self
            .records
            .read()
            .map_err(|_| "run_store_read_lock_poisoned".to_string())?
            .keys()
            .cloned()
            .collect())
    }
}
