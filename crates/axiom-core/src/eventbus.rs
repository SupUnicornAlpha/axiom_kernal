use axiom_spec::Event;

pub trait EventBus {
    fn publish(&mut self, event: Event);
    fn snapshot(&self) -> Vec<Event>;
}

#[derive(Default, Clone, Debug)]
pub struct InMemoryEventBus {
    events: Vec<Event>,
}

impl InMemoryEventBus {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }
}

impl EventBus for InMemoryEventBus {
    fn publish(&mut self, event: Event) {
        self.events.push(event);
    }

    fn snapshot(&self) -> Vec<Event> {
        self.events.clone()
    }
}
