use axiom_spec::{RunSpec, Step};

pub trait Scheduler {
    fn next<'a>(&self, spec: &'a RunSpec, next_step_index: usize) -> Option<&'a Step>;
}

#[derive(Default)]
pub struct QueueScheduler;

impl Scheduler for QueueScheduler {
    fn next<'a>(&self, spec: &'a RunSpec, next_step_index: usize) -> Option<&'a Step> {
        spec.steps.get(next_step_index)
    }
}
