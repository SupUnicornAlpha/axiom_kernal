use axiom_spec::{CapabilityLease, Effect, RunSpec, RunState};

use crate::{CapabilityContext, CapabilityRegistry};

pub trait CapabilityTransport {
    fn invoke(
        &self,
        capability_leases: &[CapabilityLease],
        capability_id: &str,
        input: &str,
        run_spec: &RunSpec,
        run_state: &RunState,
    ) -> Result<Effect, String>;
}

pub struct LocalTransport {
    registry: CapabilityRegistry,
}

impl LocalTransport {
    pub fn new(registry: CapabilityRegistry) -> Self {
        Self { registry }
    }
}

impl CapabilityTransport for LocalTransport {
    fn invoke(
        &self,
        capability_leases: &[CapabilityLease],
        capability_id: &str,
        input: &str,
        run_spec: &RunSpec,
        run_state: &RunState,
    ) -> Result<Effect, String> {
        if !CapabilityRegistry::is_leased(capability_leases, capability_id) {
            return Err(format!("capability_denied:{capability_id}"));
        }

        let ctx = CapabilityContext {
            run_spec,
            run_state,
        };
        self.registry.invoke(capability_id, input, &ctx)
    }
}
