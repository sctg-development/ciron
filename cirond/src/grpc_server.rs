use ciron_common::{
    CironDaemon, DaemonInfo, GetLogsRequest, GetProcessStatusRequest, GetStatusRequest,
    GetStatusResponse, LogEntry, LogLevel, ProcessConfig, ProcessEvent as ProtoProcessEvent,
    ProcessState, ProcessStatus as ProtoProcessStatus, ReloadConfigRequest, ReloadConfigResponse,
    RestartProcessRequest, RestartProcessResponse, ShutdownRequest, ShutdownResponse,
    StartProcessRequest, StartProcessResponse, StopProcessRequest, StopProcessResponse,
    StreamEventsRequest, TransportInfo, TransportType,
};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::process::{LogLine, LogSource, ProcessManager};

fn to_log_entry(line: &LogLine) -> LogEntry {
    LogEntry {
        timestamp: line.timestamp_ms.to_string(),
        level: match line.source {
            LogSource::Stdout => LogLevel::Info as i32,
            LogSource::Stderr => LogLevel::Warn as i32,
        },
        source: line.source.as_str().to_string(),
        message: line.message.clone(),
    }
}

pub struct CironDaemonService {
    manager: Arc<RwLock<ProcessManager>>,
    start_time: SystemTime,
    version: String,
    config_file: String,
    transport: String,
}

impl CironDaemonService {
    pub fn new(
        manager: Arc<RwLock<ProcessManager>>,
        config_file: String,
        transport: String,
    ) -> Self {
        Self {
            manager,
            start_time: SystemTime::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            config_file,
            transport,
        }
    }

    fn get_uptime_seconds(&self) -> i32 {
        self.start_time
            .elapsed()
            .map(|d| d.as_secs() as i32)
            .unwrap_or(0)
    }

    fn parse_transport_info(&self) -> TransportInfo {
        let (transport_type, address) =
            if self.transport.starts_with("inet://") || self.transport.starts_with("tcp://") {
                (TransportType::Inet, self.transport.clone())
            } else if self.transport.starts_with("unix://") {
                (TransportType::Unix, self.transport.clone())
            } else if self.transport.starts_with("vsock://") {
                (TransportType::Vsock, self.transport.clone())
            } else {
                (TransportType::Unspecified, self.transport.clone())
            };

        TransportInfo {
            r#type: transport_type as i32,
            address,
        }
    }
}

#[tonic::async_trait]
impl CironDaemon for CironDaemonService {
    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        info!("Received GetStatus request");

        let (processes, daemon_info) = {
            let manager = self.manager.read().await;
            let processes = manager.list_processes();

            let running_count = processes.iter().filter(|(_, running)| *running).count();
            let stopped_count = processes.len() - running_count;

            let daemon_info = DaemonInfo {
                version: self.version.clone(),
                started_at: self
                    .start_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs().to_string())
                    .unwrap_or_default(),
                uptime_seconds: self.get_uptime_seconds(),
                config_file: self.config_file.clone(),
                total_processes: processes.len() as i32,
                running_processes: running_count as i32,
                stopped_processes: stopped_count as i32,
                transport: Some(self.parse_transport_info()),
            };

            (processes, daemon_info)
        };

        let process_statuses: Vec<ProtoProcessStatus> = processes
            .into_iter()
            .map(|(name, running)| ProtoProcessStatus {
                name: name.clone(),
                state: if running {
                    ProcessState::Running
                } else {
                    ProcessState::Stopped
                } as i32,
                pid: None, // TODO: get actual PID
                started_at: None,
                uptime_seconds: None,
                restart_count: None,
                config: Some(ProcessConfig {
                    command: String::new(), // TODO: get from config
                    autostart: false,
                    restart_policy: None,
                    env: Default::default(),
                    working_directory: None,
                    user: None,
                    group: None,
                }),
            })
            .collect();

        Ok(Response::new(GetStatusResponse {
            processes: process_statuses,
            daemon_info: Some(daemon_info),
        }))
    }

    async fn get_process_status(
        &self,
        request: Request<GetProcessStatusRequest>,
    ) -> Result<Response<ProtoProcessStatus>, Status> {
        let name = &request.into_inner().name;
        info!("Received GetProcessStatus request for: {}", name);

        let manager = self.manager.read().await;
        let processes = manager.list_processes();

        let process = processes
            .iter()
            .find(|(n, _)| n == name)
            .ok_or_else(|| Status::not_found(format!("Process '{}' not found", name)))?;

        Ok(Response::new(ProtoProcessStatus {
            name: name.clone(),
            state: if process.1 {
                ProcessState::Running
            } else {
                ProcessState::Stopped
            } as i32,
            pid: None,
            started_at: None,
            uptime_seconds: None,
            restart_count: None,
            config: Some(ProcessConfig {
                command: String::new(),
                autostart: false,
                restart_policy: None,
                env: Default::default(),
                working_directory: None,
                user: None,
                group: None,
            }),
        }))
    }

    async fn start_process(
        &self,
        request: Request<StartProcessRequest>,
    ) -> Result<Response<StartProcessResponse>, Status> {
        let name = request.into_inner().name;
        info!("Received StartProcess request for: {}", name);

        let mut manager = self.manager.write().await;

        match manager.start_process(&name).await {
            Ok(_) => {
                info!("Successfully started process: {}", name);
                Ok(Response::new(StartProcessResponse {
                    success: true,
                    message: format!("Process '{}' started successfully", name),
                    status: Some(ProtoProcessStatus {
                        name: name.clone(),
                        state: ProcessState::Running as i32,
                        pid: None,
                        started_at: None,
                        uptime_seconds: None,
                        restart_count: None,
                        config: None,
                    }),
                }))
            }
            Err(e) => {
                error!("Failed to start process {}: {}", name, e);
                Ok(Response::new(StartProcessResponse {
                    success: false,
                    message: format!("Failed to start process '{}': {}", name, e),
                    status: None,
                }))
            }
        }
    }

    async fn stop_process(
        &self,
        request: Request<StopProcessRequest>,
    ) -> Result<Response<StopProcessResponse>, Status> {
        let req = request.into_inner();
        let name = req.name;
        info!(
            "Received StopProcess request for: {} (force: {})",
            name, req.force
        );

        let mut manager = self.manager.write().await;

        match manager.stop_process(&name).await {
            Ok(_) => {
                info!("Successfully stopped process: {}", name);
                Ok(Response::new(StopProcessResponse {
                    success: true,
                    message: format!("Process '{}' stopped successfully", name),
                }))
            }
            Err(e) => {
                error!("Failed to stop process {}: {}", name, e);
                Ok(Response::new(StopProcessResponse {
                    success: false,
                    message: format!("Failed to stop process '{}': {}", name, e),
                }))
            }
        }
    }

    async fn restart_process(
        &self,
        request: Request<RestartProcessRequest>,
    ) -> Result<Response<RestartProcessResponse>, Status> {
        let name = request.into_inner().name;
        info!("Received RestartProcess request for: {}", name);

        let mut manager = self.manager.write().await;

        match manager.restart_process(&name).await {
            Ok(_) => {
                info!("Successfully restarted process: {}", name);
                Ok(Response::new(RestartProcessResponse {
                    success: true,
                    message: format!("Process '{}' restarted successfully", name),
                    status: Some(ProtoProcessStatus {
                        name: name.clone(),
                        state: ProcessState::Running as i32,
                        pid: None,
                        started_at: None,
                        uptime_seconds: None,
                        restart_count: None,
                        config: None,
                    }),
                }))
            }
            Err(e) => {
                error!("Failed to restart process {}: {}", name, e);
                Ok(Response::new(RestartProcessResponse {
                    success: false,
                    message: format!("Failed to restart process '{}': {}", name, e),
                    status: None,
                }))
            }
        }
    }

    type GetLogsStream = tokio_stream::wrappers::ReceiverStream<Result<LogEntry, Status>>;

    async fn get_logs(
        &self,
        request: Request<GetLogsRequest>,
    ) -> Result<Response<Self::GetLogsStream>, Status> {
        let req = request.into_inner();
        let name = req.name;
        info!(
            "Received GetLogs request for: {} (follow: {}, lines: {})",
            name, req.follow, req.lines
        );

        let manager = self.manager.read().await;

        let lines = if req.lines <= 0 { 0 } else { req.lines as usize };
        let recent = manager
            .get_recent_logs(&name, lines)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;

        let follow_rx = if req.follow {
            Some(
                manager
                    .subscribe_logs(&name)
                    .map_err(|e| Status::not_found(e.to_string()))?,
            )
        } else {
            None
        };

        drop(manager);

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            for line in &recent {
                if tx.send(Ok(to_log_entry(line))).await.is_err() {
                    return;
                }
            }

            let Some(mut follow_rx) = follow_rx else {
                return;
            };

            loop {
                match follow_rx.recv().await {
                    Ok(line) => {
                        if tx.send(Ok(to_log_entry(&line))).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn reload_config(
        &self,
        _request: Request<ReloadConfigRequest>,
    ) -> Result<Response<ReloadConfigResponse>, Status> {
        unimplemented!()
    }

    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        unimplemented!()
    }

    type StreamEventsStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoProcessEvent, Status>>;

    async fn stream_events(
        &self,
        _request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        unimplemented!()
    }
}
