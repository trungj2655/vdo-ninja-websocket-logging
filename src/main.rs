use clap::Parser;
use duration_human::{DurationHuman, DurationHumanValidator};
use futures_util::{SinkExt, StreamExt};
use std::{fs, path::Path, error::Error, time::Duration};
use tokio::{signal, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::*;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::EnvFilter;
use chrono::Local;
clap_duration::assign_duration_range_validator!(TIMEOUT_RANGE = {default: 1s, min: 1s, max: 100s});
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "wss://api.vdo.ninja:443")]
    url: String,

    #[arg(short, long)]
    api_id: String,

    #[arg(short, long, default_value = "./logs")]
    logs_dir: String,

    #[arg(short, long,
    help = format!("Specify the reconnection timeout, between {}", TIMEOUT_RANGE),
    default_value = TIMEOUT_RANGE.default,
    value_parser = {|timeout: &str| TIMEOUT_RANGE.parse_and_validate(timeout)}
    )]
    timeout: DurationHuman,
}
enum ConnectionStatus {
    Retry,
    Shutdown,
}
struct LocalTimer;
impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%d %H:%M:%S%.6f"))
    }
}
#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = fs::create_dir_all(&args.logs_dir) {
        panic!("Failed to create log directory: {}", e);
    }
    let file_name = format!("{}/{}.log", &args.logs_dir, Local::now().format("%Y%m%d_%H%M%S%.6f"));
    let file_path = Path::new(&file_name);
    let file = fs::File::create(file_path).expect("Failed to create log file");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking)
        .with_timer(LocalTimer)
        .with_target(false)
        .with_ansi(false)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    let _main_span = info_span!("main").entered();
    let handshake = serde_json::json!({"join": &args.api_id}).to_string();
    let timeout: Duration = (&args.timeout).into();
    info!(?file_path, %handshake, ?timeout);
    loop {
        info!(url = ?args.url, "Connecting...");
        match run_websocket_client(&args.url, &handshake).await {
            Ok(ConnectionStatus::Shutdown) => {
                info!("Graceful shutdown complete. Exiting...");
                break;
            },
            Ok(ConnectionStatus::Retry) => {
                warn!(?timeout, "Connection closed by server. Retrying...");
            },
            Err(e) => {
                error!(error = ?e, ?timeout, "Connection error. Retrying...");
            },
        }
        sleep(timeout).await;
    }
}
#[instrument(skip_all)]
async fn run_websocket_client(url: &str, handshake: &str) -> Result<ConnectionStatus, Box<dyn Error>> {
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();
    info!("Connected");
    write.send(Message::Text(handshake.into())).await?;
    info!(%handshake, "Handshake data sent");
    loop {
        tokio::select! {
            msg_result = read.next() => {
                match msg_result {
                    Some(Ok(msg)) => {
                        match msg {
                            Message::Text(text) => info!("{}", text),
                            Message::Binary(bin) => info!(length = bin.len(), "BIN"),
                            Message::Close(_) => {
                                warn!("Server closed connection");
                                return Ok(ConnectionStatus::Retry);
                            },
                            _ => {},
                        }
                    },
                    Some(Err(e)) => return Err(Box::new(e)),
                    None => return Ok(ConnectionStatus::Retry),
                }
            },
            _ = signal::ctrl_c() => {
                warn!("Shutdown signal received (Ctrl+C). Initiating graceful shutdown...");
                write.send(Message::Close(None)).await?;
                return Ok(ConnectionStatus::Shutdown);
            },
        }
    }
}