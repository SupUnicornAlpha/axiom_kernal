use std::collections::HashMap;

use axiom_spec::{CapabilityLease, Effect, RunSpec, RunState};

#[derive(Clone, Debug)]
pub struct CapabilityContext<'a> {
    pub run_spec: &'a RunSpec,
    pub run_state: &'a RunState,
}

pub trait CapabilityDriver {
    fn invoke(&self, input: &str, ctx: &CapabilityContext<'_>) -> Result<Effect, String>;
}

pub struct StaticCapability {
    handler: Box<dyn Fn(&str, &CapabilityContext<'_>) -> Result<Effect, String> + Send + Sync>,
}

impl StaticCapability {
    pub fn new<F>(handler: F) -> Self
    where
        F: Fn(&str, &CapabilityContext<'_>) -> Result<Effect, String> + Send + Sync + 'static,
    {
        Self {
            handler: Box::new(handler),
        }
    }
}

impl CapabilityDriver for StaticCapability {
    fn invoke(&self, input: &str, ctx: &CapabilityContext<'_>) -> Result<Effect, String> {
        (self.handler)(input, ctx)
    }
}

pub struct CapabilityRegistry {
    drivers: HashMap<String, Box<dyn CapabilityDriver + Send + Sync>>,
}

impl Default for CapabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self {
            drivers: HashMap::new(),
        }
    }

    pub fn register<D>(&mut self, capability_id: impl Into<String>, driver: D)
    where
        D: CapabilityDriver + Send + Sync + 'static,
    {
        self.drivers.insert(capability_id.into(), Box::new(driver));
    }

    pub fn contains(&self, capability_id: &str) -> bool {
        self.drivers.contains_key(capability_id)
    }

    pub fn invoke(
        &self,
        capability_id: &str,
        input: &str,
        ctx: &CapabilityContext<'_>,
    ) -> Result<Effect, String> {
        self.drivers
            .get(capability_id)
            .ok_or_else(|| format!("capability_not_found:{capability_id}"))?
            .invoke(input, ctx)
    }

    pub fn is_leased(leasable: &[CapabilityLease], capability_id: &str) -> bool {
        leasable
            .iter()
            .any(|lease| lease.capability_id == capability_id)
    }
}
