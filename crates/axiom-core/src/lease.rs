use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterLease {
    pub run_id: String,
    pub holder_id: String,
    pub epoch: u64,
    pub expires_at_ms: u64,
}

pub trait RunLeaseStore: Send + Sync {
    fn acquire(
        &self,
        run_id: &str,
        holder_id: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WriterLease, String>;

    fn renew(&self, lease: &WriterLease, now_ms: u64, ttl_ms: u64) -> Result<WriterLease, String>;

    fn validate(&self, lease: &WriterLease, now_ms: u64) -> Result<(), String>;

    fn release(&self, lease: &WriterLease) -> Result<(), String>;
}

pub trait AtomicLeaseBackend: Send + Sync {
    fn server_now_ms(&self) -> Result<u64, String>;
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    fn compare_exchange(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, String>;
}

#[derive(Clone)]
pub struct DistributedRunLeaseStore<B> {
    backend: B,
    key_prefix: String,
}
pub type PostgresRunLeaseStore<B> = DistributedRunLeaseStore<B>;
pub type EtcdRunLeaseStore<B> = DistributedRunLeaseStore<B>;

impl<B> DistributedRunLeaseStore<B> {
    pub fn new(backend: B, key_prefix: impl Into<String>) -> Self {
        Self {
            backend,
            key_prefix: key_prefix.into(),
        }
    }
    fn key(&self, run_id: &str) -> String {
        format!("{}{}", self.key_prefix, run_id)
    }
}

impl<B: AtomicLeaseBackend> DistributedRunLeaseStore<B> {
    fn mutate<T>(
        &self,
        run_id: &str,
        operation: impl Fn(Option<WriterLease>, u64) -> Result<(WriterLease, T), String>,
    ) -> Result<T, String> {
        let key = self.key(run_id);
        for _ in 0..32 {
            let current_bytes = self.backend.get(&key)?;
            let current = current_bytes
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()
                .map_err(|error| error.to_string())?;
            let (next, result) = operation(current, self.backend.server_now_ms()?)?;
            let next_bytes = serde_json::to_vec(&next).map_err(|error| error.to_string())?;
            if self
                .backend
                .compare_exchange(&key, current_bytes.as_deref(), &next_bytes)?
            {
                return Ok(result);
            }
        }
        Err("distributed_lease_cas_contention".to_string())
    }
}

impl<B: AtomicLeaseBackend> RunLeaseStore for DistributedRunLeaseStore<B> {
    fn acquire(
        &self,
        run_id: &str,
        holder_id: &str,
        _worker_now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WriterLease, String> {
        self.mutate(run_id, |previous, now| {
            if let Some(active) = &previous {
                if active.expires_at_ms > now && active.holder_id != holder_id {
                    return Err(format!(
                        "writer_lease_held:{}:{}:{}",
                        run_id, active.holder_id, active.epoch
                    ));
                }
            }
            let epoch = match previous {
                Some(active) if active.holder_id == holder_id && active.expires_at_ms > now => {
                    active.epoch
                }
                Some(active) => active.epoch + 1,
                None => 1,
            };
            let lease = WriterLease {
                run_id: run_id.to_string(),
                holder_id: holder_id.to_string(),
                epoch,
                expires_at_ms: now.saturating_add(ttl_ms),
            };
            Ok((lease.clone(), lease))
        })
    }
    fn renew(
        &self,
        lease: &WriterLease,
        _worker_now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WriterLease, String> {
        self.mutate(&lease.run_id, |current, now| {
            validate_current(current.as_ref(), lease, now)?;
            let renewed = WriterLease {
                expires_at_ms: now.saturating_add(ttl_ms),
                ..lease.clone()
            };
            Ok((renewed.clone(), renewed))
        })
    }
    fn validate(&self, lease: &WriterLease, _worker_now_ms: u64) -> Result<(), String> {
        let current = self
            .backend
            .get(&self.key(&lease.run_id))?
            .ok_or_else(|| format!("writer_lease_missing:{}", lease.run_id))?;
        let current: WriterLease =
            serde_json::from_slice(&current).map_err(|error| error.to_string())?;
        validate_current(Some(&current), lease, self.backend.server_now_ms()?)
    }
    fn release(&self, lease: &WriterLease) -> Result<(), String> {
        self.mutate(&lease.run_id, |current, now| {
            validate_current(current.as_ref(), lease, now)?;
            Ok((
                WriterLease {
                    expires_at_ms: 0,
                    ..lease.clone()
                },
                (),
            ))
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryRunLeaseStore {
    leases: Arc<RwLock<BTreeMap<String, WriterLease>>>,
}

impl MemoryRunLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RunLeaseStore for MemoryRunLeaseStore {
    fn acquire(
        &self,
        run_id: &str,
        holder_id: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WriterLease, String> {
        let mut leases = self
            .leases
            .write()
            .map_err(|_| "lease_store_write_lock_poisoned".to_string())?;
        let previous = leases.get(run_id).cloned();
        if let Some(active) = &previous {
            if active.expires_at_ms > now_ms && active.holder_id != holder_id {
                return Err(format!(
                    "writer_lease_held:{}:{}:{}",
                    run_id, active.holder_id, active.epoch
                ));
            }
        }
        let epoch = match previous {
            Some(active) if active.holder_id == holder_id && active.expires_at_ms > now_ms => {
                active.epoch
            }
            Some(active) => active.epoch + 1,
            None => 1,
        };
        let lease = WriterLease {
            run_id: run_id.to_string(),
            holder_id: holder_id.to_string(),
            epoch,
            expires_at_ms: now_ms.saturating_add(ttl_ms),
        };
        leases.insert(run_id.to_string(), lease.clone());
        Ok(lease)
    }

    fn renew(&self, lease: &WriterLease, now_ms: u64, ttl_ms: u64) -> Result<WriterLease, String> {
        let mut leases = self
            .leases
            .write()
            .map_err(|_| "lease_store_write_lock_poisoned".to_string())?;
        validate_current(leases.get(&lease.run_id), lease, now_ms)?;
        let renewed = WriterLease {
            expires_at_ms: now_ms.saturating_add(ttl_ms),
            ..lease.clone()
        };
        leases.insert(lease.run_id.clone(), renewed.clone());
        Ok(renewed)
    }

    fn validate(&self, lease: &WriterLease, now_ms: u64) -> Result<(), String> {
        let leases = self
            .leases
            .read()
            .map_err(|_| "lease_store_read_lock_poisoned".to_string())?;
        validate_current(leases.get(&lease.run_id), lease, now_ms)
    }

    fn release(&self, lease: &WriterLease) -> Result<(), String> {
        let mut leases = self
            .leases
            .write()
            .map_err(|_| "lease_store_write_lock_poisoned".to_string())?;
        validate_current(
            leases.get(&lease.run_id),
            lease,
            lease.expires_at_ms.saturating_sub(1),
        )?;
        leases.insert(
            lease.run_id.clone(),
            WriterLease {
                expires_at_ms: 0,
                ..lease.clone()
            },
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FileRunLeaseStore {
    root: PathBuf,
}

impl FileRunLeaseStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn lease_path(&self, run_id: &str) -> PathBuf {
        self.root.join(format!("{}.lease", hex_id(run_id)))
    }

    fn with_locked_lease<T>(
        &self,
        run_id: &str,
        operation: impl FnOnce(Option<WriterLease>) -> Result<(Option<WriterLease>, T), String>,
    ) -> Result<T, String> {
        fs::create_dir_all(&self.root).map_err(|err| err.to_string())?;
        let path = self.lease_path(run_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|err| err.to_string())?;
        file.lock_exclusive().map_err(|err| err.to_string())?;
        let result = (|| {
            let mut contents = String::new();
            file.read_to_string(&mut contents)
                .map_err(|err| err.to_string())?;
            let current = if contents.trim().is_empty() {
                None
            } else {
                Some(serde_json::from_str(&contents).map_err(|err| err.to_string())?)
            };
            let (updated, value) = operation(current)?;
            file.set_len(0).map_err(|err| err.to_string())?;
            file.seek(SeekFrom::Start(0))
                .map_err(|err| err.to_string())?;
            if let Some(updated) = updated {
                let bytes = serde_json::to_vec(&updated).map_err(|err| err.to_string())?;
                file.write_all(&bytes).map_err(|err| err.to_string())?;
            }
            file.flush().map_err(|err| err.to_string())?;
            file.sync_all().map_err(|err| err.to_string())?;
            Ok(value)
        })();
        let unlock_result = FileExt::unlock(&file).map_err(|err| err.to_string());
        match (result, unlock_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }
}

impl RunLeaseStore for FileRunLeaseStore {
    fn acquire(
        &self,
        run_id: &str,
        holder_id: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WriterLease, String> {
        self.with_locked_lease(run_id, |previous| {
            if let Some(active) = &previous {
                if active.expires_at_ms > now_ms && active.holder_id != holder_id {
                    return Err(format!(
                        "writer_lease_held:{}:{}:{}",
                        run_id, active.holder_id, active.epoch
                    ));
                }
            }
            let epoch = match previous {
                Some(active) if active.holder_id == holder_id && active.expires_at_ms > now_ms => {
                    active.epoch
                }
                Some(active) => active.epoch + 1,
                None => 1,
            };
            let lease = WriterLease {
                run_id: run_id.to_string(),
                holder_id: holder_id.to_string(),
                epoch,
                expires_at_ms: now_ms.saturating_add(ttl_ms),
            };
            Ok((Some(lease.clone()), lease))
        })
    }

    fn renew(&self, lease: &WriterLease, now_ms: u64, ttl_ms: u64) -> Result<WriterLease, String> {
        self.with_locked_lease(&lease.run_id, |current| {
            validate_current(current.as_ref(), lease, now_ms)?;
            let renewed = WriterLease {
                expires_at_ms: now_ms.saturating_add(ttl_ms),
                ..lease.clone()
            };
            Ok((Some(renewed.clone()), renewed))
        })
    }

    fn validate(&self, lease: &WriterLease, now_ms: u64) -> Result<(), String> {
        self.with_locked_lease(&lease.run_id, |current| {
            validate_current(current.as_ref(), lease, now_ms)?;
            Ok((current, ()))
        })
    }

    fn release(&self, lease: &WriterLease) -> Result<(), String> {
        self.with_locked_lease(&lease.run_id, |current| {
            validate_current(
                current.as_ref(),
                lease,
                lease.expires_at_ms.saturating_sub(1),
            )?;
            Ok((
                Some(WriterLease {
                    expires_at_ms: 0,
                    ..lease.clone()
                }),
                (),
            ))
        })
    }
}

fn validate_current(
    current: Option<&WriterLease>,
    lease: &WriterLease,
    now_ms: u64,
) -> Result<(), String> {
    let current = current.ok_or_else(|| format!("writer_lease_missing:{}", lease.run_id))?;
    if current.holder_id != lease.holder_id || current.epoch != lease.epoch {
        return Err(format!(
            "writer_fenced:{}:{}:{}:{}:{}",
            lease.run_id, lease.holder_id, lease.epoch, current.holder_id, current.epoch
        ));
    }
    if current.expires_at_ms <= now_ms {
        return Err(format!(
            "writer_lease_expired:{}:{}:{}",
            lease.run_id, lease.holder_id, lease.epoch
        ));
    }
    Ok(())
}

fn hex_id(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod distributed_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone, Default)]
    struct Backend {
        now: Arc<AtomicU64>,
        values: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    }
    impl AtomicLeaseBackend for Backend {
        fn server_now_ms(&self) -> Result<u64, String> {
            Ok(self.now.load(Ordering::SeqCst))
        }
        fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(self.values.read().unwrap().get(key).cloned())
        }
        fn compare_exchange(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            value: &[u8],
        ) -> Result<bool, String> {
            let mut values = self.values.write().unwrap();
            if values.get(key).map(Vec::as_slice) != expected {
                return Ok(false);
            }
            values.insert(key.to_string(), value.to_vec());
            Ok(true)
        }
    }

    #[test]
    fn server_clock_fences_workers_with_clock_skew() {
        let backend = Backend::default();
        backend.now.store(1_000, Ordering::SeqCst);
        let store = DistributedRunLeaseStore::new(backend.clone(), "lease/");
        let first = store.acquire("run", "a", 9_999_999, 100).unwrap();
        assert!(store.acquire("run", "b", 0, 100).is_err());
        backend.now.store(1_101, Ordering::SeqCst);
        let second = store.acquire("run", "b", 0, 100).unwrap();
        assert_eq!(second.epoch, first.epoch + 1);
        assert!(store.validate(&first, 99_999_999).is_err());
    }
}
