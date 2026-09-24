mod grpc_server;
mod log_format;
mod process;

use ciron_common::{CironDaemonServer, Transport, load_config};
use clap::Parser;
use grpc_server::CironDaemonService;
use process::ProcessManager;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[cfg(unix)]
use {tokio::net::UnixListener, tokio_stream::wrappers::UnixListenerStream};

#[cfg(target_os = "linux")]
use {
    std::pin::Pin,
    std::task::{Context, Poll},
    tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener, VsockStream},
    tonic::transport::server::Connected,
};

// Wrapper to implement Connected trait for VsockStream
#[cfg(target_os = "linux")]
struct VsockConnection {
    stream: VsockStream,
}

#[cfg(target_os = "linux")]
impl Connected for VsockConnection {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

#[cfg(target_os = "linux")]
impl tokio::io::AsyncRead for VsockConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[cfg(target_os = "linux")]
impl tokio::io::AsyncWrite for VsockConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[derive(Parser)]
#[command(name = "cirond")]
#[command(about = "Ciron process manager daemon", long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "ciron.toml")]
    config: String,

    /// Run in foreground (don't daemonize)
    #[arg(short, long)]
    foreground: bool,

    /// Override transport address (inet://host:port, unix:///path, vsock://cid:port)
    /// If not specified, uses transport config from config file
    #[arg(short, long)]
    transport: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .event_format(log_format::CironFormatter)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    info!("Starting Cirond process manager daemon");
    info!("Loading configuration from: {}", cli.config);

    // Load configuration
    let config = load_config(&cli.config)?;
    info!("Configuration loaded successfully");
    info!("Log level: {:?}", config.log_level);
    info!("Programs configured: {}", config.program.len());

    // Determine transport to use
    let transports = if let Some(transport_str) = &cli.transport {
        info!("Using transport from CLI: {}", transport_str);
        vec![Transport::parse(transport_str)?]
    } else {
        let mut transports = Vec::new();

        if config.transport.enable_unix {
            #[cfg(unix)]
            {
                transports.push(Transport::Unix {
                    path: PathBuf::from(&config.transport.unix_socket_path),
                });
                info!("Unix socket enabled: {}", config.transport.unix_socket_path);
            }
            #[cfg(not(unix))]
            {
                info!("Unix socket requested but not available on this platform");
            }
        }

        if config.transport.enable_inet {
            let parts: Vec<&str> = config.transport.inet_address.split(':').collect();
            if parts.len() == 2 {
                transports.push(Transport::Inet {
                    host: parts[0].to_string(),
                    port: parts[1].parse()?,
                });
                info!("Inet socket enabled: {}", config.transport.inet_address);
            }
        }

        if config.transport.enable_vsock {
            #[cfg(target_os = "linux")]
            {
                transports.push(Transport::Vsock {
                    cid: config.transport.vsock_cid,
                    port: config.transport.vsock_port,
                });
                info!(
                    "Vsock enabled: CID={}, port={}",
                    config.transport.vsock_cid, config.transport.vsock_port
                );
            }
            #[cfg(not(target_os = "linux"))]
            {
                info!("Vsock requested but not available on this platform (Linux only)");
            }
        }

        if transports.is_empty() {
            error!("No transports enabled in configuration");
            anyhow::bail!("At least one transport must be enabled");
        }

        transports
    };

    // Create process manager and load configuration
    let manager = ProcessManager::new();
    let manager = Arc::new(RwLock::new(manager));

    {
        let mut mgr = manager.write().await;
        mgr.load_from_config(config);
    }

    // Start all autostart processes
    {
        let mut mgr = manager.write().await;
        mgr.start_all_autostart().await?;
    }

    // List running processes
    {
        let mgr = manager.read().await;
        let processes = mgr.list_processes();
        info!("Process status:");
        for (name, running) in processes {
            info!(
                "  {} - {}",
                name,
                if running { "RUNNING" } else { "STOPPED" }
            );
        }
    }

    // Setup Ctrl+C handler
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn Ctrl+C listener
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Received Ctrl+C signal");
                let _ = shutdown_tx_clone.send(());
            }
            Err(err) => {
                error!("Error setting up Ctrl+C handler: {}", err);
            }
        }
    });

    // Clone manager for event loop
    let event_manager = manager.clone();

    // Spawn event processing task that checks periodically
    let event_loop = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            let mut mgr = event_manager.write().await;
            // Process all pending events
            while mgr.handle_single_event().await {}
            drop(mgr);
        }
    });

    // Start gRPC servers for each transport
    let mut server_handles = Vec::new();

    for transport in transports {
        let manager_clone = manager.clone();
        let config_path = cli.config.clone();
        let transport_str = transport.to_string();
        let mut shutdown_rx_clone = shutdown_rx.resubscribe();

        let handle = tokio::spawn(async move {
            let grpc_service =
                CironDaemonService::new(manager_clone, config_path, transport_str.clone());

            let result: anyhow::Result<()> = match transport {
                Transport::Inet { host, port } => {
                    let addr = format!("{}:{}", host, port).parse()?;
                    info!("Starting gRPC server on: {}", addr);

                    Server::builder()
                        .add_service(CironDaemonServer::new(grpc_service))
                        .serve_with_shutdown(addr, async move {
                            let _ = shutdown_rx_clone.recv().await;
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("Server error: {}", e))
                }
                #[cfg(unix)]
                Transport::Unix { path } => {
                    // Remove existing socket file if it exists
                    let _ = std::fs::remove_file(&path);

                    info!("Starting gRPC server on Unix socket: {}", path.display());

                    let uds = UnixListener::bind(&path)?;
                    let uds_stream = UnixListenerStream::new(uds);

                    Server::builder()
                        .add_service(CironDaemonServer::new(grpc_service))
                        .serve_with_incoming_shutdown(uds_stream, async move {
                            let _ = shutdown_rx_clone.recv().await;
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("Server error: {}", e))
                }
                #[cfg(not(unix))]
                Transport::Unix { .. } => Err(anyhow::anyhow!(
                    "Unix sockets not supported on this platform"
                )),
                #[cfg(target_os = "linux")]
                Transport::Vsock { cid, port } => {
                    info!("Starting gRPC server on Vsock: CID={}, port={}", cid, port);

                    // For server, we listen on VMADDR_CID_ANY to accept connections from any CID
                    let addr = VsockAddr::new(VMADDR_CID_ANY, port);
                    let mut listener = VsockListener::bind(addr)?;

                    // Create a stream of incoming connections
                    let incoming = async_stream::stream! {
                        loop {
                            match listener.accept().await {
                                Ok((stream, addr)) => {
                                    info!("Accepted vsock connection from {:?}", addr);
                                    yield Ok::<_, std::io::Error>(VsockConnection { stream });
                                }
                                Err(e) => {
                                    error!("Error accepting vsock connection: {}", e);
                                    yield Err(e);
                                }
                            }
                        }
                    };

                    Server::builder()
                        .add_service(CironDaemonServer::new(grpc_service))
                        .serve_with_incoming_shutdown(incoming, async move {
                            let _ = shutdown_rx_clone.recv().await;
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("Server error: {}", e))
                }
                #[cfg(not(target_os = "linux"))]
                Transport::Vsock { .. } => Err(anyhow::anyhow!(
                    "Vsock not supported on this platform (Linux only)"
                )),
            };

            result
        });

        server_handles.push(handle);
    }

    // Wait for all servers to complete
    for handle in server_handles {
        match handle.await {
            Ok(Ok(())) => {
                info!("Server stopped successfully");
            }
            Ok(Err(e)) => {
                error!("Server error: {}", e);
            }
            Err(e) => {
                error!("Server task error: {}", e);
            }
        }
    }

    info!("All gRPC servers stopped");

    // Stop all processes
    info!("Stopping all processes...");
    {
        let mut mgr = manager.write().await;
        mgr.shutdown(); // Close event channel first
        mgr.stop_all().await;
    }

    // Abort event loop
    event_loop.abort();

    info!("Cirond daemon stopped");
    Ok(())
}
