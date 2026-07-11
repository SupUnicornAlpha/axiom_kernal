use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axiom_spec::{Event, EventKind, RunSpec};

use crate::EventJournal;

pub trait WrapPolicy: Send + Sync {
    fn authorize(&self, spec: &RunSpec) -> Result<(), String>;
}

#[derive(Default)]
pub struct AllowWrap;

impl WrapPolicy for AllowWrap {
    fn authorize(&self, _spec: &RunSpec) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum WrapError {
    Denied(String),
    External(String),
    EventLog(String),
}

#[derive(Clone, Debug)]
pub struct WrapReport<T> {
    pub output: T,
    pub events: Vec<Event>,
}

pub struct WrapHarness<P = AllowWrap> {
    policy: P,
    event_log: Option<Arc<dyn EventJournal>>,
}

impl Default for WrapHarness<AllowWrap> {
    fn default() -> Self {
        Self::new(AllowWrap, None)
    }
}

impl<P: WrapPolicy> WrapHarness<P> {
    pub fn new(policy: P, event_log: Option<Arc<dyn EventJournal>>) -> Self {
        Self { policy, event_log }
    }

    pub fn run<T, F>(&self, spec: &RunSpec, external: F) -> Result<WrapReport<T>, WrapError>
    where
        F: FnOnce() -> Result<T, String>,
    {
        self.policy.authorize(spec).map_err(WrapError::Denied)?;
        let mut events = Vec::new();
        self.emit(spec, &mut events, EventKind::RunCreated, "wrap_created")?;
        self.emit(spec, &mut events, EventKind::RunStarted, "wrap_started")?;
        self.emit(
            spec,
            &mut events,
            EventKind::StepStarted,
            "external_executor_started",
        )?;
        match external() {
            Ok(output) => {
                self.emit(
                    spec,
                    &mut events,
                    EventKind::StepCompleted,
                    "external_executor_completed",
                )?;
                self.emit(spec, &mut events, EventKind::RunCompleted, "wrap_completed")?;
                Ok(WrapReport { output, events })
            }
            Err(error) => {
                self.emit(
                    spec,
                    &mut events,
                    EventKind::RunFailed,
                    format!("external_executor_failed:{error}"),
                )?;
                Err(WrapError::External(error))
            }
        }
    }

    fn emit(
        &self,
        spec: &RunSpec,
        events: &mut Vec<Event>,
        kind: EventKind,
        detail: impl Into<String>,
    ) -> Result<(), WrapError> {
        let mut event = Event::new(&spec.run_id, None, kind, detail);
        event.sequence = events.len() as u64 + 1;
        event.timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| WrapError::EventLog(error.to_string()))?
            .as_millis() as u64;
        event.spec_digest = spec.digest();
        event.writer_epoch = 1;
        event.causation_id =
            (event.sequence > 1).then(|| format!("{}:{}", spec.run_id, event.sequence - 1));
        if let Some(event_log) = &self.event_log {
            event_log.append(&event).map_err(WrapError::EventLog)?;
        }
        events.push(event);
        Ok(())
    }
}
