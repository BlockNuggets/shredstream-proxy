use crossbeam_channel::Receiver;
use jito_protos::shredstream::Entry as PbEntry;
use log::{error, info, warn};
use serde::Serialize; // 导入 Serialize trait
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

const ADDRESS: &str = "/opt/socks/proxy.sock";

/// 这是一个与 jito_protos::shredstream::Entry 结构相同的镜像结构体。
/// 我们定义它纯粹是为了能够为其派生 serde::Serialize，从而可以使用 bincode。
/// 这避免了修改 jito_protos crate 的需求。
#[derive(Serialize)]
struct BincodeEntry {
    slot: u64,
    entries: Vec<u8>,
}

impl From<PbEntry> for BincodeEntry {
    /// 实现一个转换函数，方便地将 Protobuf 类型转换为我们的 bincode 兼容类型。
    fn from(pb_entry: PbEntry) -> Self {
        Self {
            slot: pb_entry.slot,
            entries: pb_entry.entries,
        }
    }
}

pub fn start_unix_server_thread(
    entry_sender: Arc<broadcast::Sender<PbEntry>>,
    exit: Arc<AtomicBool>,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                error!("Failed to create Tokio runtime for unix_server: {}", e);
                return;
            }
        };

        let server_handle = runtime.spawn(async move {
            if fs::metadata(ADDRESS).is_ok() {
                if let Err(e) = fs::remove_file(ADDRESS) {
                    warn!(
                        "Failed to remove existing unix socket file {}: {}",
                        ADDRESS, e
                    );
                }
            }

            let listener = match UnixListener::bind(ADDRESS) {
                Ok(l) => l,
                Err(e) => {
                    error!("Failed to bind to unix socket {}: {}", ADDRESS, e);
                    return;
                }
            };
            info!("Unix server listening on {}", ADDRESS);

            loop {
                match listener.accept().await {
                    Ok((mut socket, _addr)) => {
                        info!("Accepted new unix socket client");
                        let mut entry_receiver = entry_sender.subscribe();
                        tokio::spawn(async move {
                            loop {
                                match entry_receiver.recv().await {
                                    Ok(entry) => {
                                        // 1. 将 PbEntry 转换为 BincodeEntry
                                        let bincode_entry = BincodeEntry::from(entry);

                                        // 2. 使用 bincode 序列化新的结构体
                                        match bincode::serialize(&bincode_entry) {
                                            Ok(buf) => {
                                                if let Err(e) = socket.write_all(&buf).await {
                                                    info!("Unix socket client disconnected: {}", e);
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "Failed to serialize entry with bincode: {}",
                                                    e
                                                );
                                                continue;
                                            }
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        warn!("Unix socket client lagged by {} messages", n);
                                    }
                                    Err(broadcast::error::RecvError::Closed) => {
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Failed to accept unix socket connection: {}", e);
                    }
                }
            }
        });

        while !exit.load(Ordering::Relaxed) {
            if shutdown_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
            {
                info!("Shutting down unix server");
                server_handle.abort();
                break;
            }
        }

        if let Err(e) = fs::remove_file(ADDRESS) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("Failed to clean up unix socket file {}: {}", ADDRESS, e);
            }
        }
    })
}
