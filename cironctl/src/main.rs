use anyhow::{Context, Result};
use ciron_common::{
    CironDaemonClient, GetLogsRequest, GetStatusRequest, RestartProcessRequest,
    StartProcessRequest, StopProcessRequest, Transport,
};
use clap::{Parser, Subcommand};

#[cfg(unix)]
use {hyper_util::rt::TokioIo, tokio::net::UnixStream, tower::service_fn};

#[cfg(target_os = "linux")]
use tokio_vsock::{VsockAddr, VsockStream};

#[derive(Parser)]
#[command(name = "cironctl")]
#[command(about = "Control utility for Cirond daemon", long_about = None)]
#[command(version)]
struct Cli {
    #[arg(short, long, default_value = "unix:///tmp/cirond.sock")]
    transport: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show status of all processes
    Status,

    /// Start a process
    Start { name: String },

    /// Stop a process
    Stop {
        name: String,
        #[arg(short, long)]
        force: bool,
    },

    /// Restart a process
    Restart { name: String },

    /// Show logs of a process
    Logs {
        name: String,
        #[arg(short, long)]
        follow: bool,
        /// Number of lines to show
        #[arg(short, long, default_value = "50")]
        lines: i32,
    },

    /// Reload daemon configuration
    Reload,

    /// Stop all processes and shutdown daemon
    Shutdown {
        #[arg(short, long, default_value = "true")]
        graceful: bool,

        /// Timeout in seconds
        #[arg(short, long)]
        timeout: Option<i32>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Parse transport
    let transport =
        Transport::parse(&cli.transport).context("Failed to parse transport address")?;

    // Connect to daemon based on transport type
    let mut client = match transport {
        Transport::Inet { ref host, port } => {
            let uri = format!("http://{}:{}", host, port);
            CironDaemonClient::connect(uri)
                .await
                .context("Failed to connect to cirond. Is the daemon running?")?
        }
        #[cfg(unix)]
        Transport::Unix { ref path } => {
            let path = path.clone();

            // Create a channel that connects via Unix socket
            let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
                .connect_with_connector(service_fn(move |_: tonic::transport::Uri| {
                    let path = path.clone();
                    async move {
                        UnixStream::connect(path)
                            .await
                            .map(TokioIo::new)
                            .map_err(std::io::Error::other)
                    }
                }))
                .await
                .context("Failed to connect to cirond via Unix socket. Is the daemon running?")?;

            CironDaemonClient::new(channel)
        }
        #[cfg(not(unix))]
        Transport::Unix { .. } => {
            anyhow::bail!("Unix sockets not supported on this platform");
        }
        #[cfg(target_os = "linux")]
        Transport::Vsock { cid, port } => {
            // Create a channel that connects via Vsock
            let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
                .connect_with_connector(service_fn(move |_: tonic::transport::Uri| async move {
                    let addr = VsockAddr::new(cid, port);
                    VsockStream::connect(addr)
                        .await
                        .map(TokioIo::new)
                        .map_err(std::io::Error::other)
                }))
                .await
                .context("Failed to connect to cirond via Vsock. Is the daemon running?")?;

            CironDaemonClient::new(channel)
        }
        #[cfg(not(target_os = "linux"))]
        Transport::Vsock { .. } => {
            anyhow::bail!("Vsock transport not supported on this platform (Linux only)");
        }
    };

    match cli.command {
        Commands::Status => {
            let response = client
                .get_status(GetStatusRequest {})
                .await
                .context("Failed to get status")?;

            let resp = response.into_inner();

            println!("Cirond Status");
            println!("─────────────────────────────────────────");

            if let Some(daemon_info) = resp.daemon_info {
                println!("Version: {}", daemon_info.version);
                println!("Uptime: {}s", daemon_info.uptime_seconds);
                println!("Config: {}", daemon_info.config_file);
                println!(
                    "Processes: {} total, {} running, {} stopped",
                    daemon_info.total_processes,
                    daemon_info.running_processes,
                    daemon_info.stopped_processes
                );
                println!();
            }

            println!("Processes:");
            for process in resp.processes {
                let state_str = match process.state {
                    1 => "STOPPED",
                    2 => "STARTING",
                    3 => "RUNNING",
                    4 => "STOPPING",
                    5 => "FAILED",
                    6 => "EXITED",
                    _ => "UNKNOWN",
                };

                println!("  {:20} {}", process.name, state_str);

                if let Some(pid) = process.pid {
                    println!("    PID: {}", pid);
                }
            }

            Ok(())
        }
        Commands::Start { name } => {
            println!("Starting process: {}", name);

            let response = client
                .start_process(StartProcessRequest { name: name.clone() })
                .await
                .context("Failed to start process")?;

            let resp = response.into_inner();

            if resp.success {
                println!("Success: {}", resp.message);
            } else {
                println!("Failed: {}", resp.message);
            }

            Ok(())
        }
        Commands::Stop { name, force } => {
            println!("Stopping process: {}", name);

            let response = client
                .stop_process(StopProcessRequest {
                    name: name.clone(),
                    force,
                })
                .await
                .context("Failed to stop process")?;

            let resp = response.into_inner();

            if resp.success {
                println!("Success: {}", resp.message);
            } else {
                println!("Failed: {}", resp.message);
            }

            Ok(())
        }
        Commands::Restart { name } => {
            println!("Restarting process: {}", name);

            let response = client
                .restart_process(RestartProcessRequest { name: name.clone() })
                .await
                .context("Failed to restart process")?;

            let resp = response.into_inner();

            if resp.success {
                println!("Success: {}", resp.message);
            } else {
                println!("Failed: {}", resp.message);
            }

            Ok(())
        }
        Commands::Logs {
            name,
            follow,
            lines,
        } => {
            let mut stream = client
                .get_logs(GetLogsRequest {
                    name: name.clone(),
                    follow,
                    lines,
                    since: None,
                })
                .await
                .context("Failed to get logs")?
                .into_inner();

            loop {
                match stream.message().await {
                    Ok(Some(entry)) => {
                        println!("[{}] {}", entry.source, entry.message);
                    }
                    Ok(None) => break,
                    Err(status) => {
                        return Err(anyhow::anyhow!("Log stream error: {}", status));
                    }
                }
            }

            Ok(())
        }
        _ => unimplemented!(),
    }
}
