use std::collections::BTreeMap;

pub type MetadataMap = BTreeMap<String, String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSpec {
    pub run_id: String,
    pub name: String,
    pub namespace: RunNamespace,
    pub budget: BudgetGroup,
    pub capability_leases: Vec<CapabilityLease>,
    pub steps: Vec<Step>,
    pub metadata: MetadataMap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunNamespace {
    pub workspace_root: String,
    pub visible_capabilities: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetGroup {
    pub max_steps: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityLease {
    pub capability_id: String,
    pub permissions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub id: String,
    pub title: String,
    pub action: StepAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepAction {
    Message {
        role: String,
        content: String,
    },
    CapabilityInvoke {
        capability_id: String,
        input: String,
    },
    Delegate {
        child: Box<RunSpec>,
        merge_mode: MergeMode,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeMode {
    SummaryOnly,
    AppendMessages,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunState {
    pub run_id: String,
    pub status: RunStatus,
    pub next_step_index: usize,
    pub messages: Vec<Message>,
    pub outputs: Vec<String>,
    pub denied_actions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Ready,
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind {
    RunCreated,
    RunStarted,
    StepProposed,
    ShellDecision,
    StepStarted,
    StepCompleted,
    EffectApplied,
    CapabilityDenied,
    ChildRunCreated,
    ChildRunCompleted,
    RunCompleted,
    RunFailed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub run_id: String,
    pub step_id: Option<String>,
    pub kind: EventKind,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Effect {
    pub summary: String,
    pub messages: Vec<Message>,
    pub outputs: Vec<String>,
}

impl Effect {
    pub fn empty(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            messages: Vec::new(),
            outputs: Vec::new(),
        }
    }
}

impl RunSpec {
    pub fn new(run_id: impl Into<String>, name: impl Into<String>, steps: Vec<Step>) -> Self {
        Self {
            run_id: run_id.into(),
            name: name.into(),
            namespace: RunNamespace {
                workspace_root: ".".to_string(),
                visible_capabilities: Vec::new(),
            },
            budget: BudgetGroup { max_steps: 128 },
            capability_leases: Vec::new(),
            steps,
            metadata: MetadataMap::new(),
        }
    }
}

impl RunState {
    pub fn from_spec(spec: &RunSpec) -> Self {
        Self {
            run_id: spec.run_id.clone(),
            status: RunStatus::Ready,
            next_step_index: 0,
            messages: Vec::new(),
            outputs: Vec::new(),
            denied_actions: Vec::new(),
        }
    }
}
