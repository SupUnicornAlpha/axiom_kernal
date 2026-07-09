use std::collections::BTreeMap;

use axiom_spec::RunState;

#[derive(Clone, Debug)]
pub struct RunStoreRecord {
    pub run_id: String,
    pub state: RunState,
}

pub trait RunStore {
    fn put(&mut self, record: RunStoreRecord);
    fn get(&self, run_id: &str) -> Option<RunStoreRecord>;
    fn list_run_ids(&self) -> Vec<String>;
}

#[derive(Default, Clone, Debug)]
pub struct MemoryRunStore {
    records: BTreeMap<String, RunStoreRecord>,
}

impl MemoryRunStore {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }
}

impl RunStore for MemoryRunStore {
    fn put(&mut self, record: RunStoreRecord) {
        self.records.insert(record.run_id.clone(), record);
    }

    fn get(&self, run_id: &str) -> Option<RunStoreRecord> {
        self.records.get(run_id).cloned()
    }

    fn list_run_ids(&self) -> Vec<String> {
        self.records.keys().cloned().collect()
    }
}
