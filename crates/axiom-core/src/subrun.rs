use axiom_spec::{ChildRunResult, ChildRunSpec, Effect, RunUsage};

use crate::{KernelError, RunReport};

pub trait SubRunTransport {
    fn execute(
        &self,
        child: &ChildRunSpec,
        executor: &dyn Fn(&axiom_spec::RunSpec) -> Result<RunReport, KernelError>,
    ) -> Result<ChildRunResult, KernelError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalSubRunTransport;

impl SubRunTransport for LocalSubRunTransport {
    fn execute(
        &self,
        child: &ChildRunSpec,
        executor: &dyn Fn(&axiom_spec::RunSpec) -> Result<RunReport, KernelError>,
    ) -> Result<ChildRunResult, KernelError> {
        let report = executor(&child.run)?;
        Ok(result_from_report(child, report, "local"))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RemoteSubRunTransportMock;

impl SubRunTransport for RemoteSubRunTransportMock {
    fn execute(
        &self,
        child: &ChildRunSpec,
        executor: &dyn Fn(&axiom_spec::RunSpec) -> Result<RunReport, KernelError>,
    ) -> Result<ChildRunResult, KernelError> {
        let report = executor(&child.run)?;
        Ok(result_from_report(child, report, "remote-mock"))
    }
}

fn result_from_report(child: &ChildRunSpec, report: RunReport, transport: &str) -> ChildRunResult {
    let event_count = report.events.len();
    let steps = report.state.next_step_index;
    let proposed_effect = Effect {
        summary: format!("child_result:{}", child.run.run_id),
        messages: report.state.messages,
        outputs: report.state.outputs,
    };

    ChildRunResult {
        run_id: child.run.run_id.clone(),
        status: report.state.status,
        summary: proposed_effect.summary.clone(),
        artifacts: Vec::new(),
        proposed_effects: vec![proposed_effect],
        events_digest: format!("events:{event_count}"),
        usage: RunUsage {
            steps,
            events: event_count,
        },
        diagnostics: vec![format!("transport:{transport}")],
    }
}
