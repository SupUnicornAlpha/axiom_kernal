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
        child: Box<ChildRunSpec>,
        merge_mode: MergeMode,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildRunSpec {
    pub parent_run_id: String,
    pub run: RunSpec,
    pub memory_view: Vec<String>,
    pub sandbox_profile: String,
    pub return_contract: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildRunResult {
    pub run_id: String,
    pub status: RunStatus,
    pub summary: String,
    pub artifacts: Vec<String>,
    pub proposed_effects: Vec<EffectProposal>,
    pub events_digest: String,
    pub usage: RunUsage,
    pub diagnostics: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunUsage {
    pub steps: usize,
    pub events: usize,
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
    EffectProposed,
    EffectCommitted,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectProposal {
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

impl EffectProposal {
    pub fn empty(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            messages: Vec::new(),
            outputs: Vec::new(),
        }
    }
}

impl From<EffectProposal> for Effect {
    fn from(proposal: EffectProposal) -> Self {
        Self {
            summary: proposal.summary,
            messages: proposal.messages,
            outputs: proposal.outputs,
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

impl ChildRunSpec {
    pub fn new(parent_run_id: impl Into<String>, run: RunSpec) -> Self {
        Self {
            parent_run_id: parent_run_id.into(),
            run,
            memory_view: Vec::new(),
            sandbox_profile: "default-deny".to_string(),
            return_contract: "effect-proposals-v1".to_string(),
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
