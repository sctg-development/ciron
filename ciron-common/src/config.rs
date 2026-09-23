use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
pub struct GlobalConfig {
    #[serde(default)]
    pub log_level: Option<String>,
    #[serde(default)]
    pub transport: TransportConfig,
    pub program: HashMap<String, ProgramConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TransportConfig {
    #[serde(default = "default_true")]
    pub enable_unix: bool,
    #[serde(default = "default_unix_socket_path")]
    pub unix_socket_path: String,
    #[serde(default)]
    pub enable_inet: bool,
    #[serde(default = "default_inet_address")]
    pub inet_address: String,
    #[serde(default)]
    pub enable_vsock: bool,
    #[serde(default = "default_vsock_cid")]
    pub vsock_cid: u32,
    #[serde(default = "default_vsock_port")]
    pub vsock_port: u32,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            enable_unix: true,
            unix_socket_path: default_unix_socket_path(),
            enable_inet: false,
            inet_address: default_inet_address(),
            enable_vsock: false,
            vsock_cid: default_vsock_cid(),
            vsock_port: default_vsock_port(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_unix_socket_path() -> String {
    "/tmp/cirond.sock".to_string()
}

fn default_inet_address() -> String {
    "127.0.0.1:50051".to_string()
}

fn default_vsock_cid() -> u32 {
    u32::MAX // All CIDs
}

fn default_vsock_port() -> u32 {
    50051
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProgramConfig {
    pub command: String,
    #[serde(default)]
    pub autostart: bool,
    #[serde(default)]
    pub restart: Option<String>,
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
    /// When true, this process' stdout/stderr are forwarded into cirond's own
    /// log output (visible e.g. via `kubectl logs`) and kept in a buffer so
    /// they can be queried through `GetLogs` / `cironctl logs`. Disabled by
    /// default: the pipes are still drained so the child never blocks, but
    /// nothing is recorded or logged.
    #[serde(default)]
    pub log_forward: bool,
}

pub fn load_config(path: &str) -> Result<GlobalConfig> {
    let settings = Config::builder()
        .add_source(File::with_name(path))
        .build()?;

    let config: GlobalConfig = settings.try_deserialize()?;
    Ok(config)
}
