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
        invoke_with_registry(
            &self.registry,
            capability_leases,
            capability_id,
            input,
            run_spec,
            run_state,
        )
    }
}

pub struct RemoteTransportMock {
    registry: CapabilityRegistry,
}

impl RemoteTransportMock {
    pub fn new(registry: CapabilityRegistry) -> Self {
        Self { registry }
    }
}

impl CapabilityTransport for RemoteTransportMock {
    fn invoke(
        &self,
        capability_leases: &[CapabilityLease],
        capability_id: &str,
        input: &str,
        run_spec: &RunSpec,
        run_state: &RunState,
    ) -> Result<Effect, String> {
        invoke_with_registry(
            &self.registry,
            capability_leases,
            capability_id,
            input,
            run_spec,
            run_state,
        )
    }
}

fn invoke_with_registry(
    registry: &CapabilityRegistry,
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
    registry.invoke(capability_id, input, &ctx)
}
