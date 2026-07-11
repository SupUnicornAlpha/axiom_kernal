use std::collections::VecDeque;
use std::sync::Mutex;

use axiom_spec::{RunSpec, RunState, Step, StepAction};

pub trait Scheduler {
    fn next(&self, spec: &RunSpec, state: &RunState) -> Result<Option<Step>, String>;
}

#[derive(Default)]
pub struct QueueScheduler;

impl Scheduler for QueueScheduler {
    fn next(&self, spec: &RunSpec, state: &RunState) -> Result<Option<Step>, String> {
        Ok(spec.steps.get(state.next_step_index).cloned())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelDecision {
    Invoke {
        capability_id: String,
        input: String,
    },
    Respond {
        content: String,
    },
    Finish,
}

pub trait ModelDriver: Send + Sync {
    fn decide(&self, spec: &RunSpec, state: &RunState) -> Result<ModelDecision, String>;
}

pub struct ReActScheduler<M> {
    model: M,
}

impl<M> ReActScheduler<M> {
    pub fn new(model: M) -> Self {
        Self { model }
    }
}

impl<M: ModelDriver> Scheduler for ReActScheduler<M> {
    fn next(&self, spec: &RunSpec, state: &RunState) -> Result<Option<Step>, String> {
        if let Some(step) = spec.steps.get(state.next_step_index) {
            return Ok(Some(step.clone()));
        }

        let turn = state.next_step_index + 1;
        let step = match self.model.decide(spec, state)? {
            ModelDecision::Invoke {
                capability_id,
                input,
            } => Step {
                id: format!("react-{turn}-tool"),
                title: format!("invoke {capability_id}"),
                action: StepAction::CapabilityInvoke {
                    capability_id,
                    input,
                },
            },
            ModelDecision::Respond { content } => Step {
                id: format!("react-{turn}-response"),
                title: "assistant response".to_string(),
                action: StepAction::Message {
                    role: "assistant".to_string(),
                    content,
                },
            },
            ModelDecision::Finish => return Ok(None),
        };
        Ok(Some(step))
    }
}

pub struct MockModelDriver {
    decisions: Mutex<VecDeque<ModelDecision>>,
}

impl MockModelDriver {
    pub fn scripted(decisions: impl IntoIterator<Item = ModelDecision>) -> Self {
        Self {
            decisions: Mutex::new(decisions.into_iter().collect()),
        }
    }
}

impl ModelDriver for MockModelDriver {
    fn decide(&self, _spec: &RunSpec, _state: &RunState) -> Result<ModelDecision, String> {
        self.decisions
            .lock()
            .map_err(|_| "mock_model_lock_poisoned".to_string())?
            .pop_front()
            .ok_or_else(|| "mock_model_script_exhausted".to_string())
    }
}
