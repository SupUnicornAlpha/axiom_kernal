use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use axiom_spec::RunState;

#[derive(Clone, Debug)]
pub struct RunStoreRecord {
    pub run_id: String,
    pub state: RunState,
}

pub trait RunStore: Send + Sync {
    fn put(&self, record: RunStoreRecord) -> Result<(), String>;
    fn get(&self, run_id: &str) -> Result<Option<RunStoreRecord>, String>;
    fn list_run_ids(&self) -> Result<Vec<String>, String>;
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
