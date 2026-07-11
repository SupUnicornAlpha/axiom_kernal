use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use axiom_spec::EffectProposal;
use serde::Deserialize;

use crate::{CapabilityContext, CapabilityDriver, StaticCapability};

pub type FunctionDriver = StaticCapability;

pub struct CliDriver {
    executable: PathBuf,
    fixed_args: Vec<String>,
}

impl CliDriver {
    pub fn new(executable: impl Into<PathBuf>, fixed_args: Vec<String>) -> Self {
        Self {
            executable: executable.into(),
            fixed_args,
        }
    }
}

impl CapabilityDriver for CliDriver {
    fn invoke(&self, input: &str, _ctx: &CapabilityContext<'_>) -> Result<EffectProposal, String> {
        let output = Command::new(&self.executable)
            .args(&self.fixed_args)
            .arg(input)
            .output()
            .map_err(|error| format!("cli_spawn_failed:{error}"))?;
        if !output.status.success() {
            return Err(format!(
                "cli_exit_failed:{}:{}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let stdout = String::from_utf8(output.stdout)
            .map_err(|error| format!("cli_stdout_not_utf8:{error}"))?;
        Ok(EffectProposal {
            summary: "cli_completed".to_string(),
            messages: Vec::new(),
            outputs: vec![stdout.trim_end().to_string()],
        })
    }
}

pub struct FilesystemDriver {
    root: PathBuf,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum FilesystemRequest {
    Read { path: String },
    Write { path: String, content: String },
}

impl FilesystemDriver {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|error| format!("filesystem_root_create:{error}"))?;
        let root = root
            .canonicalize()
            .map_err(|error| format!("filesystem_root_canonicalize:{error}"))?;
        Ok(Self { root })
    }

    fn resolve(&self, relative: &str) -> Result<PathBuf, String> {
        let path = Path::new(relative);
        if path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(format!("filesystem_path_denied:{relative}"));
        }
        Ok(self.root.join(path))
    }
}

impl CapabilityDriver for FilesystemDriver {
    fn invoke(&self, input: &str, _ctx: &CapabilityContext<'_>) -> Result<EffectProposal, String> {
        let request: FilesystemRequest = serde_json::from_str(input)
            .map_err(|error| format!("filesystem_request_invalid:{error}"))?;
        match request {
            FilesystemRequest::Read { path } => {
                let path = self.resolve(&path)?;
                let content = fs::read_to_string(path)
                    .map_err(|error| format!("filesystem_read_failed:{error}"))?;
                Ok(EffectProposal {
                    summary: "filesystem_read".to_string(),
                    messages: Vec::new(),
                    outputs: vec![content],
                })
            }
            FilesystemRequest::Write { path, content } => {
                let path = self.resolve(&path)?;
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|error| format!("filesystem_parent_create:{error}"))?;
                }
                fs::write(&path, content.as_bytes())
                    .map_err(|error| format!("filesystem_write_failed:{error}"))?;
                Ok(EffectProposal {
                    summary: "filesystem_write".to_string(),
                    messages: Vec::new(),
                    outputs: vec![path.to_string_lossy().to_string()],
                })
            }
        }
    }
}
