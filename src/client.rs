//! # The Asynchronous MQTT Client
//!
//! This module contains the primary `MqttClient` struct, which manages the state,
//! connection, and communication with an MQTT broker.

use crate::error::{MqttError, ProtocolError};
use crate::packet::{self, Connect, EncodePacket, MqttPacket, PingReq, Publish, QoS, Subscribe};
use crate::transport::{self, MqttTransport};
use embassy_time::{Duration, Instant};
use heapless::{String, Vec};

/// Represents the MQTT protocol version used by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttVersion {
    V3,
    V5,
}

/// Last Will and Testament configuration for MQTT CONNECT.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct LastWill<'a> {
    /// Topic the broker will publish to when the client disconnects unexpectedly.
    pub topic: &'a str,
    /// Payload the broker will publish on unexpected disconnect.
    pub payload: &'a [u8],
    /// QoS for the will publish.
    pub qos: QoS,
    /// Retain flag for the will publish.
    pub retain: bool,
}

/// Configuration options for the `MqttClient`.
pub struct MqttOptions<'a> {
    client_id: &'a str,
    version: MqttVersion,
    keep_alive: Duration,
    clean_session: bool,
    username: Option<String<32>>,
    password: Option<String<64>>,
    will: Option<LastWill<'a>>,
}

impl<'a> MqttOptions<'a> {
    pub fn new(client_id: &'a str) -> Self {
        Self {
            client_id,
            version: MqttVersion::V3,
            keep_alive: Duration::from_secs(60),
            clean_session: true,
            username: None,
            password: None,
            will: None,
        }
    }

    /// Sets the MQTT Clean Session flag.
    ///
    /// `false` keeps the broker-side session across reconnects (delivers
    /// offline QoS1 messages), as required by MXS protocol V2.1.2.
    pub fn with_clean_session(mut self, clean_session: bool) -> Self {
        self.clean_session = clean_session;
        self
    }
    #[cfg(feature = "v5")]
    pub fn with_version(mut self, version: MqttVersion) -> Self {
        self.version = version;
        self
    }
    pub fn with_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }
    /// Sets the username and password for MQTT broker authentication.
    ///
    /// Username is limited to 32 bytes, password to 64 bytes.
    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.username = String::try_from(username).ok();
        self.password = String::try_from(password).ok();
        self
    }

    /// Sets the MQTT Last Will and Testament message.
    pub fn with_last_will(mut self, will: LastWill<'a>) -> Self {
        self.will = Some(will);
        self
    }
}

/// Maximum number of receive attempts when waiting for PUBACK/SUBACK.
/// Skips interleaved packets (PingResp, Publish). Prevents infinite loop on broken connection.
const MAX_RECV_ATTEMPTS: usize = 16;

/// 返回缓冲中第一个 MQTT 报文的总线长（固定头 1B + 剩余长度变长字段 + 剩余长度）。
/// 用于在同一 TCP 段合并多包时正确切分；不足一个完整变长头或变长非法时返回 None。
fn first_packet_len(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }
    let mut multiplier = 1u32;
    let mut value = 0u32;
    let mut i = 1usize; // 跳过首字节（报文类型/标志）
    loop {
        if i >= buf.len() {
            return None;
        }
        let b = buf[i];
        value += ((b & 0x7f) as u32) * multiplier;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        multiplier = multiplier.checked_mul(128)?;
        if i > 5 {
            return None;
        }
    }
    Some(1 + (i - 1) + value as usize)
}

/// 首包的分类（拥有所有权，不借用缓冲）。用于在拆包循环里决定如何推进游标，
/// 避免「返回事件 / 推进 rx_off」与「缓冲借用」的冲突。
enum PktKind {
    PubAck,
    SubAck { packet_id: u16, failed: bool },
    PingResp,
    Publish,
    Other,
}

/// Maximum topic length for runtime-provided Last Will messages.
const MAX_WILL_TOPIC_LEN: usize = 128;
/// Maximum payload length for runtime-provided Last Will messages.
const MAX_WILL_PAYLOAD_LEN: usize = 256;

/// Owned storage for a runtime-provided Last Will message.
struct OwnedLastWill {
    topic: String<MAX_WILL_TOPIC_LEN>,
    payload: Vec<u8, MAX_WILL_PAYLOAD_LEN>,
    qos: QoS,
    retain: bool,
}

/// Represents the current connection state of the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
}

/// The asynchronous MQTT client.
pub struct MqttClient<'a, T, const MAX_TOPICS: usize, const BUF_SIZE: usize>
where
    T: MqttTransport,
{
    transport: T,
    options: MqttOptions<'a>,
    tx_buffer: [u8; BUF_SIZE],
    rx_buffer: [u8; BUF_SIZE],
    // 收包游标：rx_buffer[rx_off..rx_len] 为尚未解析的字节（可能含同一 TCP 段合并的多个包）。
    // 缓冲耗尽（rx_off == rx_len）时 poll()/publish() 重新 recv 并从头部填充；不足一个完整包时
    // 继续 recv 追加，按 MQTT 变长头「已知长度」逐包解析，无需任何额外缓存。
    rx_off: usize,
    rx_len: usize,
    state: ConnectionState,
    last_tx_time: Instant,
    next_packet_id: u16,
    runtime_will: Option<OwnedLastWill>,
}

impl<'a, T, const MAX_TOPICS: usize, const BUF_SIZE: usize> MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>
where
    T: MqttTransport,
{
    pub fn new(transport: T, options: MqttOptions<'a>) -> Self {
        Self {
            transport,
            options,
            tx_buffer: [0; BUF_SIZE],
            rx_buffer: [0; BUF_SIZE],
            rx_off: 0,
            rx_len: 0,
            state: ConnectionState::Disconnected,
            last_tx_time: Instant::now(),
            next_packet_id: 1,
            runtime_will: None,
        }
    }

    /// Sets/overrides the Last Will and Testament for the next connections.
    ///
    /// Returns `false` when topic or payload exceed internal fixed buffers.
    pub fn set_last_will(&mut self, will: LastWill<'_>) -> bool {
        let mut topic: String<MAX_WILL_TOPIC_LEN> = String::new();
        if topic.push_str(will.topic).is_err() {
            return false;
        }

        let mut payload: Vec<u8, MAX_WILL_PAYLOAD_LEN> = Vec::new();
        if payload.extend_from_slice(will.payload).is_err() {
            return false;
        }

        self.runtime_will = Some(OwnedLastWill {
            topic,
            payload,
            qos: will.qos,
            retain: will.retain,
        });
        true
    }

    /// Attempts to connect to the MQTT broker.
    pub async fn connect(&mut self) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        self.state = ConnectionState::Connecting;
        let will = if let Some(will) = self.runtime_will.as_ref() {
            Some(LastWill {
                topic: will.topic.as_str(),
                payload: will.payload.as_slice(),
                qos: will.qos,
                retain: will.retain,
            })
        } else {
            self.options.will
        };
        let connect_packet = Connect::with_credentials(
            self.options.client_id,
            self.options.keep_alive.as_secs() as u16,
            self.options.clean_session,
            self.options.username.as_deref(),
            self.options.password.as_ref().map(|s| s.as_bytes()),
            will,
        );
        let len = connect_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;

        self.transport.send(&self.tx_buffer[..len]).await?;

        let n = self.transport.recv(&mut self.rx_buffer).await?;

        let packet = packet::decode::<T::Error>(&self.rx_buffer[..n], self.options.version);

        let packet = packet?.ok_or(MqttError::Protocol(ProtocolError::InvalidResponse))?;

        if let MqttPacket::ConnAck(connack) = packet {
            if connack.reason_code == 0 {
                self.state = ConnectionState::Connected;
                self.last_tx_time = Instant::now();
                Ok(())
            } else {
                self.state = ConnectionState::Disconnected;
                Err(MqttError::ConnectionRefused(connack.reason_code.into()))
            }
        } else {
            self.state = ConnectionState::Disconnected;
            Err(MqttError::Protocol(ProtocolError::InvalidResponse))
        }
    }

    /// Publishes a message to a topic.
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
    ) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        self.publish_with_retain(topic, payload, qos, false).await
    }

    /// Publishes a message to a topic, with explicit retain flag.
    pub async fn publish_with_retain(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let packet_id = if qos != QoS::AtMostOnce {
            Some(self.get_next_packet_id())
        } else {
            None
        };

        let publish = Publish {
            topic,
            qos,
            retain,
            payload,
            packet_id,
            #[cfg(feature = "v5")]
            properties: heapless::Vec::new(),
        };

        let len = publish
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();

        // Wait for PUBACK if QoS > 0; skip interleaved PingResp/Publish to avoid race with keep-alive.
        // broker 可能把 PUBACK 与下行 Publish（如 upgrade_query 的 DOWN 回复）合并进同一 TCP 段，
        // 需逐包解析：遇到 PUBACK 即完成握手，并把段内残留字节暂存到 rx_leftover 由 poll() 消费，
        // 否则残留的下行包会随下一次 recv() 被覆盖丢弃。
        if qos != QoS::AtMostOnce {
            for _ in 0..MAX_RECV_ATTEMPTS {
                if self.rx_off >= self.rx_len {
                    self.rx_off = 0;
                    self.rx_len = 0;
                }
                match self.peek_kind()? {
                    None => {
                        // 不足一个完整包：recv 追加到缓冲末尾，下一轮再解析
                        let n = self.transport.recv(&mut self.rx_buffer[self.rx_len..]).await?;
                        self.rx_len += n;
                        continue;
                    }
                    Some((PktKind::PubAck, total)) => {
                        self.rx_off += total;
                        #[cfg(feature = "log")]
                        log::info!("PUBACK received, QoS1 publish handshake completed");
                        return Ok(());
                    }
                    Some((PktKind::PingResp, total)) => {
                        self.rx_off += total;
                        continue;
                    }
                    Some((PktKind::Publish, total)) => {
                        // 下行包在等 PUBACK 期间到达：保留在 rx_buffer 中，由 poll() 后续从 rx_off
                        // 处解析。实际 PUBACK 必先于下行到达，正常在 PubAck 分支即返回；此处顺序异常
                        // 时前移游标跳过（极端边缘，该 Publish 不交给 poll）。
                        self.rx_off += total;
                        continue;
                    }
                    Some((_, _)) => return Err(MqttError::Protocol(ProtocolError::InvalidResponse)),
                }
            }
            return Err(MqttError::Protocol(ProtocolError::InvalidResponse));
        }

        Ok(())
    }

    /// Subscribes to a topic with specified QoS.
    pub async fn subscribe(&mut self, topic: &str, qos: QoS) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let packet_id = self.get_next_packet_id();
        let subscribe = Subscribe::new(packet_id, topic, qos);

        let len = subscribe
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();

        // Wait for SUBACK; skip interleaved PingResp/Publish（与 publish 一致的游标拆包）
        for _ in 0..MAX_RECV_ATTEMPTS {
            if self.rx_off >= self.rx_len {
                self.rx_off = 0;
                self.rx_len = 0;
            }
            match self.peek_kind()? {
                None => {
                    let n = self.transport.recv(&mut self.rx_buffer[self.rx_len..]).await?;
                    self.rx_len += n;
                    continue;
                }
                Some((PktKind::SubAck { packet_id: pid, failed }, total)) => {
                    if failed || pid != packet_id {
                        return Err(MqttError::Protocol(ProtocolError::InvalidResponse));
                    }
                    self.rx_off += total;
                    return Ok(());
                }
                Some((PktKind::PingResp, total)) => {
                    self.rx_off += total;
                    continue;
                }
                Some((PktKind::Publish, total)) => {
                    self.rx_off += total;
                    continue;
                }
                Some((_, _)) => return Err(MqttError::Protocol(ProtocolError::InvalidResponse)),
            }
        }
        Err(MqttError::Protocol(ProtocolError::InvalidResponse))
    }

    /// Sends a pre-constructed packet over the transport.
    async fn _send_packet<P>(&mut self, packet: P) -> Result<(), MqttError<T::Error>>
    where
        P: EncodePacket,
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }
        let len = packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(())
    }

/// 查看 `rx_buffer[rx_off..rx_len]` 处的第一个完整报文，**仅返回拥有所有权的类型描述与总线长**
/// （不持有对缓冲的借用），调用方据此推进游标、必要时重新 decode 取真实报文，从而规避借用冲突。
/// - `Ok(Some((kind, total)))`：解析到一个完整包，`total` 为其总线长；
/// - `Ok(None)`：字节不足（首部或整包未收全），需继续 `recv` 追加；
/// - `Err`：协议错误。
fn peek_kind(&self) -> Result<Option<(PktKind, usize)>, MqttError<T::Error>>
where
    T::Error: transport::TransportError,
{
    let avail = self.rx_len - self.rx_off;
    if avail < 2 {
        return Ok(None);
    }
    let buf = &self.rx_buffer[self.rx_off..self.rx_len];
    let total = match first_packet_len(buf) {
        Some(l) => l,
        None => return Ok(None), // 变长头不完整，需更多字节
    };
    if avail < total {
        return Ok(None); // 整包未收全，需继续 recv
    }
    let kind = match packet::decode::<T::Error>(&buf[..total], self.options.version)? {
        Some(MqttPacket::PubAck(_)) => PktKind::PubAck,
        Some(MqttPacket::SubAck(s)) => {
            let failed = s
                .reason_codes
                .first()
                .map(|&c| c >= 0x80)
                .unwrap_or(true);
            PktKind::SubAck {
                packet_id: s.packet_id,
                failed,
            }
        }
        Some(MqttPacket::PingResp) => PktKind::PingResp,
        Some(MqttPacket::Publish(_)) => PktKind::Publish,
        _ => PktKind::Other,
    };
    Ok(Some((kind, total)))
}

    /// 向 `rx_buffer` 末尾追加更多字节（写入 `[rx_len..]`），返回新增字节数；超时无数据返回 `0`。
    /// 用 `recv_timeout`，等待时长取「保活剩余时间」与 `max_wait` 的较小者。
    async fn recv_more(&mut self, max_wait: Duration) -> Result<usize, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        // 只取一次 elapsed：若「保活未到期」判断与「剩余时间计算」之间跨过保活边界，
        // `keep_alive - elapsed` 会下溢 panic——这是 0 超时击穿会话之外的另一崩溃形态。
        let since_tx = self.last_tx_time.elapsed();
        if since_tx >= self.options.keep_alive {
            // 保活已到期：立即返回 0，由 poll 补发 PINGREQ。绝不能把 0 作为超时
            // 传给 transport——std 的 set_read_timeout(Some(0)) 返回 InvalidInput
            // （"cannot set a 0 duration timeout"），会直接击穿 MQTT 会话造成断线。
            return Ok(0);
        }
        let wait: std::time::Duration = std::time::Duration::from(self.options.keep_alive - since_tx)
            .min(std::time::Duration::from(max_wait));

        let buf = &mut self.rx_buffer[self.rx_len..];
        match self.transport.recv_timeout(buf, wait).await {
            Ok(n) => Ok(n),
            Err(e) => Err(e.into()),
        }
    }

    /// Polls the connection for incoming packets and handles keep-alives.
    ///
    /// `max_wait` 是本次轮询的上限（由 runtime 传入 tick 截止时间）。std 构建下，保活与 tick
    /// 均按真实时钟推进（std 环境由 `std::time` 驱动，`futures::block_on` 下保活/tick 照常工作）。
    ///
    /// The returned `MqttEvent` contains references to the client's internal receive
    /// buffer. These references are only valid until the next call to `poll`.
    pub async fn poll<'p>(
        &'p mut self,
        max_wait: Duration,
    ) -> Result<Option<MqttEvent<'p>>, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        // 在 rx_buffer 上按 MQTT 变长头「已知长度」逐包解析（游标 rx_off/rx_len）。
        // 同一 TCP 段合并的多个包会被依次解析：每次 poll 处理一个完整包，剩余字节留在缓冲，
        // 由后续 poll 从 rx_off 处继续消费，绝不丢弃。不足一个完整包时 recv 追加。
        // 先判断缓冲里是否已有完整包（仅用长度信息算拥有所有权的 bool，不持有缓冲借用）。
        let has_full = {
            let avail = self.rx_len - self.rx_off;
            if avail < 2 {
                false
            } else {
                match first_packet_len(&self.rx_buffer[self.rx_off..self.rx_len]) {
                    Some(total) => avail >= total, // 已有完整包
                    None => false,                 // 变长头都不完整
                }
            }
        };

        if !has_full {
            // 字节不足一个完整包：缓冲耗尽则压缩到头部，再 recv 追加（带保活超时）
            if self.rx_off >= self.rx_len {
                self.rx_off = 0;
                self.rx_len = 0;
            }
            let got = self.recv_more(max_wait).await?;
            if got == 0 {
                // 超时无数据：达保活时刻则发 PINGREQ，否则视为 tick 唤醒
                if self.last_tx_time.elapsed() >= self.options.keep_alive {
                    self._send_packet(PingReq).await?;
                }
                return Ok(None);
            }
            self.rx_len += got;
        }

        // 此刻缓冲里应有一个完整包：先取长度（仅 usize，不持有缓冲借用），推进游标后再解析返回，
        // 避免「返回事件对 self 的借用」与「推进 rx_off 的可变借用」冲突。
        let total = match first_packet_len(&self.rx_buffer[self.rx_off..self.rx_len]) {
            Some(l) => l,
            None => return Ok(None), // 仍不完整，等下次 poll 再 recv
        };
        let start = self.rx_off;
        self.rx_off += total;
        match packet::decode::<T::Error>(&self.rx_buffer[start..self.rx_off], self.options.version)? {
            Some(MqttPacket::Publish(packet)) => Ok(Some(MqttEvent::Publish(packet))),
            _ => Ok(None), // 非 Publish（PingResp 等）跳过，下次 poll 继续消费后续包
        }
    }

    fn get_next_packet_id(&mut self) -> u16 {
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        self.next_packet_id
    }
}

/// Represents an event received from the MQTT broker.
/// The lifetime `'p` indicates that the event borrows data from the client's
/// buffer and is only valid for the duration of the `poll` call.
#[derive(Debug)]
pub enum MqttEvent<'p> {
    Publish(Publish<'p>),
}
