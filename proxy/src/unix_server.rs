use crossbeam_channel::Receiver;
use jito_protos::shredstream::Entry as PbEntry;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize}; // 导入 Serialize 和 Deserialize trait
use std::collections::HashSet;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::net::UnixDatagram;
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{timeout, Duration as TokioDuration};

const ADDRESS: &str = "/opt/socks/proxy.sock";
const BROADCAST_ADDRESS: &str = "/opt/socks/proxy_broadcast.sock";
const RECV_TIMEOUT: TokioDuration = TokioDuration::from_millis(100); // 100ms 超时

/// 这是一个与 jito_protos::shredstream::Entry 结构相同的镜像结构体。
/// 我们定义它纯粹是为了能够为其派生 serde::Serialize，从而可以使用 bincode。
/// 这避免了修改 jito_protos crate 的需求。
#[derive(Debug, Serialize, Clone)]
pub struct BincodeEntry {
    pub slot: u64,
    pub entries: Vec<u8>,
}

/// Ping-Pong 协议消息类型
#[derive(Debug, Deserialize, Serialize)]
enum PingPongMessage {
    Ping { client_id: Option<String> }, // 可选客户端ID
    Pong,
}

/// 客户端管理器
#[derive(Debug)]
struct ClientManager {
    clients: HashSet<String>,
}

impl ClientManager {
    fn new() -> Self {
        Self {
            clients: HashSet::new(),
        }
    }

    /// 添加客户端地址
    fn add_client(&mut self, address: String) -> bool {
        if self.clients.insert(address.clone()) {
            info!(
                "Registered new client: {} (total: {})",
                address,
                self.clients.len()
            );
            true
        } else {
            debug!("Client already registered: {}", address);
            false
        }
    }

    /// 移除客户端地址
    fn remove_client(&mut self, address: &str) -> bool {
        if self.clients.remove(address) {
            info!(
                "Removed client: {} (total: {})",
                address,
                self.clients.len()
            );
            true
        } else {
            false
        }
    }

    /// 获取所有客户端地址
    fn get_clients(&self) -> Vec<String> {
        self.clients.iter().cloned().collect()
    }

    /// 获取客户端数量
    fn client_count(&self) -> usize {
        self.clients.len()
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

        // 清理旧的套接字文件
        for address in [ADDRESS, BROADCAST_ADDRESS] {
            if fs::metadata(address).is_ok() {
                if let Err(e) = fs::remove_file(address) {
                    warn!(
                        "Failed to remove existing unix socket file {}: {}",
                        address, e
                    );
                }
            }
        }

        info!("Unix datagram server listening on {}", ADDRESS);

        // 创建客户端管理器
        let client_manager = Arc::new(Mutex::new(ClientManager::new()));

        // 启动 Ping-Pong 客户端注册任务
        let ping_pong_handle = {
            let client_manager = client_manager.clone();
            let socket = match UnixDatagram::bind(ADDRESS) {
                Ok(socket) => socket,
                Err(e) => {
                    error!("Failed to bind ping-pong socket to {}: {}", ADDRESS, e);
                    return;
                }
            };

            runtime.spawn(async move {
                let mut buffer = [0u8; 1024]; // 1KB buffer for ping-pong messages

                loop {
                    match timeout(RECV_TIMEOUT, socket.recv_from(&mut buffer)).await {
                        Ok(Ok((size, addr))) => {
                            // 正确处理客户端地址
                            let client_addr = match addr.as_pathname() {
                                Some(path) => match path.to_str() {
                                    Some(addr_str) => addr_str.to_string(),
                                    None => {
                                        warn!("Invalid client address encoding, skipping");
                                        continue;
                                    }
                                },
                                None => {
                                    warn!("Received message from non-pathname address, skipping");
                                    continue;
                                }
                            };

                            if size > 0 {
                                // 创建只包含实际接收数据的切片，避免脏数据
                                let received_data = &buffer[..size];

                                // 尝试解析 Ping 消息
                                match bincode::deserialize::<PingPongMessage>(received_data) {
                                    Ok(PingPongMessage::Ping { client_id }) => {
                                        debug!(
                                            "Received ping from {} (client_id: {:?})",
                                            client_addr, client_id
                                        );

                                        // 注册客户端（重复注册是安全的）
                                        let is_new_client = client_manager
                                            .lock()
                                            .await
                                            .add_client(client_addr.clone());

                                        if is_new_client {
                                            info!("New client registered: {}", client_addr);
                                        } else {
                                            debug!("Client re-registered: {}", client_addr);
                                        }

                                        // 发送 Pong 回复
                                        match bincode::serialize(&PingPongMessage::Pong) {
                                            Ok(pong_buf) => {
                                                if let Err(e) =
                                                    socket.send_to(&pong_buf, &client_addr).await
                                                {
                                                    warn!(
                                                        "Failed to send pong to {}: {}",
                                                        client_addr, e
                                                    );
                                                } else {
                                                    debug!("Sent pong to {}", client_addr);
                                                }
                                            }
                                            Err(e) => {
                                                warn!("Failed to serialize pong message: {}", e);
                                            }
                                        }
                                    }
                                    Ok(PingPongMessage::Pong) => {
                                        debug!("Received pong from {} (unexpected)", client_addr);
                                    }
                                    Err(e) => {
                                        debug!(
                                            "Received non-ping message from {}: {}",
                                            client_addr, e
                                        );
                                    }
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            warn!("Failed to receive ping-pong message: {}", e);
                            break;
                        }
                        Err(_) => {
                            // 超时，继续循环（这里相当于 sleep 了 RECV_TIMEOUT 时间）
                            continue;
                        }
                    }
                }
            })
        };

        // 启动数据广播任务
        let broadcast_handle = {
            let client_manager = client_manager.clone();
            let socket = match UnixDatagram::bind(BROADCAST_ADDRESS) {
                Ok(socket) => socket,
                Err(e) => {
                    error!(
                        "Failed to bind broadcast socket to {}: {}",
                        BROADCAST_ADDRESS, e
                    );
                    return;
                }
            };
            let mut entry_receiver = entry_sender.subscribe();

            runtime.spawn(async move {
                loop {
                    match entry_receiver.recv().await {
                        Ok(entry) => {
                            // 1. 将 PbEntry 转换为 BincodeEntry
                            let bincode_entry = BincodeEntry {
                                slot: entry.slot,
                                entries: entry.entries,
                            };
                            // 2. 使用 bincode 序列化新的结构体
                            match bincode::serialize(&bincode_entry) {
                                Ok(buf) => {
                                    // 3. 广播到所有已注册的客户端
                                    let clients = client_manager.lock().await.get_clients();
                                    let mut failed_clients = Vec::new();

                                    for client_addr in &clients {
                                        match socket.send_to(&buf, client_addr).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                warn!(
                                                    "Failed to send data to {}: {}",
                                                    client_addr, e
                                                );
                                                failed_clients.push(client_addr.clone());
                                            }
                                        }
                                    }

                                    // 移除发送失败的客户端
                                    if !failed_clients.is_empty() {
                                        let mut mgr = client_manager.lock().await;
                                        for failed_client in failed_clients {
                                            mgr.remove_client(&failed_client);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to serialize entry with bincode: {}", e);
                                    continue;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Unix datagram server lagged by {} messages", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
            })
        };

        // 直接保存任务句柄，不需要额外的等待任务

        while !exit.load(Ordering::Relaxed) {
            if shutdown_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
            {
                info!("Shutting down unix datagram server");
                // 直接停止两个任务，不需要额外的等待任务
                ping_pong_handle.abort();
                broadcast_handle.abort();
                break;
            }
        }

        // 程序退出时清理套接字文件
        for address in [ADDRESS, BROADCAST_ADDRESS] {
            if let Err(e) = fs::remove_file(address) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to clean up unix socket file {}: {}", address, e);
                }
            }
        }
    })
}
