use anyhow::{Context, Result};
use ciron_common::{GlobalConfig, ProgramConfig};
use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Number of log lines kept in memory per process for `GetLogs` requests.
const LOG_BUFFER_CAPACITY: usize = 1000;

#[derive(Debug, Clone)]
pub enum ProcessEvent {
    Started(String),
    Exited(String, i32),
    Failed(String, String),
    RestartRequested(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSource {
    Stdout,
    Stderr,
}

impl LogSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogSource::Stdout => "stdout",
            LogSource::Stderr => "stderr",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogLine {
    pub timestamp_ms: u128,
    pub source: LogSource,
    pub message: String,
}

pub struct ProcessManager {
    processes: HashMap<String, ManagedProcess>,
    event_tx: mpsc::UnboundedSender<ProcessEvent>,
    event_rx: mpsc::UnboundedReceiver<ProcessEvent>,
}

struct ManagedProcess {
    _name: String,
    config: ProgramConfig,
    monitor_handle: Option<JoinHandle<()>>,
    pid: Option<u32>,
    running: bool,
    log_buffer: Arc<Mutex<VecDeque<LogLine>>>,
    log_tx: broadcast::Sender<LogLine>,
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Reads a child process' stdout/stderr line by line, forwards each line to the
/// daemon's own tracing output (so it shows up alongside cirond's logs, e.g. in
/// `kubectl logs`), and records it in the in-memory ring buffer / broadcast
/// channel used to serve `GetLogs` requests.
///
/// When `forward` is false (the default, `log_forward = false`), the pipe is
/// still drained so the child never blocks on a full stdout/stderr buffer, but
/// nothing is logged or recorded.
fn spawn_log_reader<R>(
    name: String,
    source: LogSource,
    reader: R,
    log_buffer: Arc<Mutex<VecDeque<LogLine>>>,
    log_tx: broadcast::Sender<LogLine>,
    forward: bool,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if !forward {
            let mut reader = reader;
            let _ = tokio::io::copy(&mut reader, &mut tokio::io::sink()).await;
            return;
        }

        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(message)) => {
                    info!(target: "cirond::child", process = %name, stream = source.as_str(), "{}", message);

                    let entry = LogLine {
                        timestamp_ms: now_millis(),
                        source,
                        message,
                    };

                    {
                        let mut buf = log_buffer.lock().await;
                        if buf.len() >= LOG_BUFFER_CAPACITY {
                            buf.pop_front();
                        }
                        buf.push_back(entry.clone());
                    }

                    let _ = log_tx.send(entry);
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(
                        "Error reading {} log stream for {}: {}",
                        source.as_str(),
                        name,
                        e
                    );
                    break;
                }
            }
        }
    });
}

impl ProcessManager {
    pub fn new() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            processes: HashMap::new(),
            event_tx,
            event_rx,
        }
    }

    pub fn load_from_config(&mut self, config: GlobalConfig) {
        for (name, program_config) in config.program {
            info!("Loaded program configuration: {}", name);
            let (log_tx, _) = broadcast::channel(LOG_BUFFER_CAPACITY);
            self.processes.insert(
                name.clone(),
                ManagedProcess {
                    _name: name,
                    config: program_config,
                    monitor_handle: None,
                    pid: None,
                    running: false,
                    log_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(LOG_BUFFER_CAPACITY))),
                    log_tx,
                },
            );
        }
    }

    pub async fn start_all_autostart(&mut self) -> Result<()> {
        let programs_to_start: Vec<String> = self
            .processes
            .iter()
            .filter(|(_, p)| p.config.autostart)
            .map(|(name, _)| name.clone())
            .collect();

        for name in programs_to_start {
            if let Err(e) = self.start_process(&name).await {
                error!("Failed to start autostart program {}: {}", name, e);
            }
        }

        Ok(())
    }

    pub async fn start_process(&mut self, name: &str) -> Result<()> {
        let process = self
            .processes
            .get_mut(name)
            .context(format!("Program {} not found", name))?;

        if process.running {
            warn!("Process {} is already running", name);
            return Ok(());
        }

        info!("Starting process: {}", name);

        // Parse command and arguments using shell-words for proper quote handling
        let parts = shell_words::split(&process.config.command)
            .context(format!("Failed to parse command for {}", name))?;

        if parts.is_empty() {
            return Err(anyhow::anyhow!("Empty command for {}", name));
        }

        let mut cmd = Command::new(&parts[0]);
        if parts.len() > 1 {
            cmd.args(&parts[1..]);
        }

        // Set environment variables if specified
        if let Some(env) = &process.config.env {
            for (key, value) in env {
                cmd.env(key, value);
            }
        }

        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null());

        let mut child = cmd
            .spawn()
            .context(format!("Failed to spawn process {}", name))?;

        let pid = child.id();
        info!("Process {} started with PID: {:?}", name, pid);

        // Store the PID
        process.pid = pid;

        // Always drain stdout/stderr so the child never blocks on a full pipe
        // buffer; only forward/record the output when log_forward is enabled.
        let log_forward = process.config.log_forward;
        if let Some(stdout) = child.stdout.take() {
            spawn_log_reader(
                name.to_string(),
                LogSource::Stdout,
                stdout,
                process.log_buffer.clone(),
                process.log_tx.clone(),
                log_forward,
            );
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_reader(
                name.to_string(),
                LogSource::Stderr,
                stderr,
                process.log_buffer.clone(),
                process.log_tx.clone(),
                log_forward,
            );
        }

        let _ = self.event_tx.send(ProcessEvent::Started(name.to_string()));

        // Start monitoring the process
        let name_clone = name.to_string();
        let event_tx = self.event_tx.clone();

        let monitor_handle = tokio::spawn(async move {
            match child.wait().await {
                Ok(status) => {
                    let code = status.code().unwrap_or(-1);
                    info!("Process {} exited with code: {}", name_clone, code);
                    let _ = event_tx.send(ProcessEvent::Exited(name_clone.clone(), code));
                }
                Err(e) => {
                    error!("Error waiting for process {}: {}", name_clone, e);
                    let _ = event_tx.send(ProcessEvent::Failed(name_clone.clone(), e.to_string()));
                }
            }
        });

        process.monitor_handle = Some(monitor_handle);
        process.running = true;

        Ok(())
    }

    pub async fn stop_process(&mut self, name: &str) -> Result<()> {
        let process = self
            .processes
            .get_mut(name)
            .context(format!("Program {} not found", name))?;

        if !process.running {
            warn!("Process {} is not running", name);
            return Ok(());
        }

        info!("Stopping process: {}", name);

        // Send SIGTERM to the process
        if let Some(pid) = process.pid {
            #[cfg(unix)]
            {
                use nix::sys::signal::{Signal, kill};
                use nix::unistd::Pid;

                match kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                    Ok(_) => info!("Sent SIGTERM to process {} (PID: {})", name, pid),
                    Err(e) => warn!(
                        "Failed to send SIGTERM to process {} (PID: {}): {}",
                        name, pid, e
                    ),
                }
            }

            #[cfg(not(unix))]
            {
                warn!("Signal handling not implemented for non-Unix systems");
            }
        }

        // Cancel the monitor task
        if let Some(handle) = process.monitor_handle.take() {
            handle.abort();
        }

        process.running = false;
        process.pid = None;

        Ok(())
    }

    pub async fn restart_process(&mut self, name: &str) -> Result<()> {
        info!("Restarting process: {}", name);
        self.stop_process(name).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        self.start_process(name).await?;
        Ok(())
    }

    pub async fn handle_single_event(&mut self) -> bool {
        match self.event_rx.try_recv() {
            Ok(event) => {
                match event {
                    ProcessEvent::Started(name) => {
                        info!("Event: Process {} started", name);
                    }
                    ProcessEvent::Exited(name, code) => {
                        info!("Event: Process {} exited with code {}", name, code);

                        // Mark process as not running
                        if let Some(process) = self.processes.get_mut(&name) {
                            process.running = false;

                            // Handle restart policy
                            let should_restart = match process.config.restart.as_deref() {
                                Some("always") => true,
                                Some("on-failure") => code != 0,
                                _ => false,
                            };

                            if should_restart {
                                info!(
                                    "Scheduling restart for process {} due to restart policy",
                                    name
                                );
                                let event_tx = self.event_tx.clone();
                                let name_clone = name.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                                    let _ =
                                        event_tx.send(ProcessEvent::RestartRequested(name_clone));
                                });
                            }
                        }
                    }
                    ProcessEvent::Failed(name, error) => {
                        error!("Event: Process {} failed: {}", name, error);
                        if let Some(process) = self.processes.get_mut(&name) {
                            process.running = false;
                        }
                    }
                    ProcessEvent::RestartRequested(name) => {
                        info!("Event: Restart requested for {}", name);
                        if let Err(e) = self.start_process(&name).await {
                            error!("Failed to restart process {}: {}", name, e);
                        }
                    }
                }
                true
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => false,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                info!("Event channel closed");
                false
            }
        }
    }

    pub fn list_processes(&self) -> Vec<(String, bool)> {
        self.processes
            .iter()
            .map(|(name, process)| (name.clone(), process.running))
            .collect()
    }

    /// Returns up to `lines` most recent buffered log lines for a process
    /// (all buffered lines if `lines` is 0).
    pub async fn get_recent_logs(&self, name: &str, lines: usize) -> Result<Vec<LogLine>> {
        let process = self
            .processes
            .get(name)
            .context(format!("Program {} not found", name))?;

        if !process.config.log_forward {
            anyhow::bail!(
                "Log forwarding is disabled for process '{}': set log_forward = true in its configuration to enable GetLogs/cironctl logs",
                name
            );
        }

        let buf = process.log_buffer.lock().await;
        let start = if lines == 0 || lines >= buf.len() {
            0
        } else {
            buf.len() - lines
        };

        Ok(buf.iter().skip(start).cloned().collect())
    }

    /// Subscribes to new log lines produced by a process as they are emitted.
    pub fn subscribe_logs(&self, name: &str) -> Result<broadcast::Receiver<LogLine>> {
        let process = self
            .processes
            .get(name)
            .context(format!("Program {} not found", name))?;

        if !process.config.log_forward {
            anyhow::bail!(
                "Log forwarding is disabled for process '{}': set log_forward = true in its configuration to enable GetLogs/cironctl logs",
                name
            );
        }

        Ok(process.log_tx.subscribe())
    }

    pub async fn stop_all(&mut self) {
        info!("Stopping all processes");
        let names: Vec<String> = self.processes.keys().cloned().collect();
        for name in names {
            if let Err(e) = self.stop_process(&name).await {
                error!("Failed to stop process {}: {}", name, e);
            }
        }
    }

    pub fn shutdown(&mut self) {
        info!("Shutting down process manager");
        self.event_rx.close();
    }
}
