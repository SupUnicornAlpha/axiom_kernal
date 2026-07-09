use std::env;
use std::path::PathBuf;

use axiom_core::{AuditShell, CapabilityRegistry, JsonlEventLog, Kernel, QueueScheduler, StaticCapability};
use axiom_spec::{CapabilityLease, Effect, Message, RunSpec, Step, StepAction};

fn main() {
    let command = env::args().nth(1).unwrap_or_else(|| "demo".to_string());
    match command.as_str() {
        "demo" => run_demo(),
        other => {
            eprintln!("unknown command: {other}");
            std::process::exit(2);
        }
    }
}

fn run_demo() {
    let mut registry = CapabilityRegistry::new();
    registry.register(
        "tool/echo",
        StaticCapability::new(|input, _ctx| {
            Ok(Effect {
                summary: "tool_echo".to_string(),
                messages: vec![Message {
                    role: "tool".to_string(),
                    content: format!("echo:{input}"),
                }],
                outputs: vec![input.to_string()],
            })
        }),
    );

    let mut spec = RunSpec::new(
        "demo-run",
        "demo",
        vec![
            Step {
                id: "step-1".to_string(),
                title: "user prompt".to_string(),
                action: StepAction::Message {
                    role: "user".to_string(),
                    content: "please echo hello".to_string(),
                },
            },
            Step {
                id: "step-2".to_string(),
                title: "invoke echo".to_string(),
                action: StepAction::CapabilityInvoke {
                    capability_id: "tool/echo".to_string(),
                    input: "hello".to_string(),
                },
            },
        ],
    );
    spec.capability_leases.push(CapabilityLease {
        capability_id: "tool/echo".to_string(),
        permissions: vec!["invoke".to_string()],
    });

    let log_path = PathBuf::from("target/demo-events.jsonl");
    let kernel = Kernel::new(
        QueueScheduler,
        AuditShell,
        registry,
        Some(JsonlEventLog::new(log_path)),
    );

    match kernel.run(&spec) {
        Ok(report) => {
            println!("run_id={}", report.state.run_id);
            println!("status={:?}", report.state.status);
            println!("messages={}", report.state.messages.len());
            println!("outputs={:?}", report.state.outputs);
            println!("events={}", report.events.len());
        }
        Err(err) => {
            eprintln!("run failed: {err:?}");
            std::process::exit(1);
        }
    }
}
