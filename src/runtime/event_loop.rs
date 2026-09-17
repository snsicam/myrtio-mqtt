//! MQTT Runtime - drives modules and handles the event loop.

use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::channel::Receiver;
use embassy_time::{Duration, Instant};

use super::publisher::{BufferedOutbox, PublishRequest};
use super::registry::TopicRegistry;
use super::traits::MqttModule;
use crate::client::MqttClient;
use crate::error::MqttError;
use crate::transport::{MqttTransport, TransportError};
use crate::{MqttEvent, QoS};
use std::sync::atomic::{AtomicBool, Ordering};

/// The MQTT runtime that drives modules and handles the event loop.
///
/// The runtime owns the `MqttClient` and multiplexes between:
/// - Incoming MQTT messages (dispatched to modules)
/// - Outgoing publish requests from controllers (via channel)
/// - Periodic ticks for module housekeeping
///
/// # Object-Safe Module Support
///
/// The runtime is generic over the module type `M`, which must implement
/// `MqttModule`. Because `MqttModule` is object-safe, you can use:
///
/// - Concrete module types for maximum performance
/// - `&mut dyn MqttModule` for trait objects (no `Box` needed)
///
/// # Topic Registration
///
/// During startup, the runtime calls `module.register()` with a `TopicCollector`
/// to collect all topics the module wants to subscribe to. Topics are copied
/// into the registry, so they only need to live for the duration of the call.
///
/// # Publishing Pattern
///
/// Modules use a `BufferedOutbox` to queue publish requests during `on_tick`
/// and `on_start`. The runtime then drains the outbox and performs the actual
/// async publishing.
pub struct MqttRuntime<
    'a,
    T,
    M,
    const MAX_TOPICS: usize,
    const BUF_SIZE: usize,
    const OUTBOX_DEPTH: usize,
> where
    T: MqttTransport,
    M: MqttModule,
{
    client: MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>,
    module: M,
    publisher_rx: Receiver<'a, CriticalSectionRawMutex, PublishRequest<'a>, OUTBOX_DEPTH>,
}

/// Constants for the internal publish outbox used during module callbacks.
const OUTBOX_CAPACITY: usize = 8;
const OUTBOX_TOPIC_SIZE: usize = 128;
const OUTBOX_PAYLOAD_SIZE: usize = 1024;

impl<'a, T, M, const MAX_TOPICS: usize, const BUF_SIZE: usize, const OUTBOX_DEPTH: usize>
    MqttRuntime<'a, T, M, MAX_TOPICS, BUF_SIZE, OUTBOX_DEPTH>
where
    T: MqttTransport,
    T::Error: TransportError,
    M: MqttModule,
{
    /// Create a new MQTT runtime.
    ///
    /// # Arguments
    ///
    /// - `client`: The MQTT client (not yet connected)
    /// - `module`: The module (or composed modules) to drive
    /// - `publisher_rx`: Receiver end of the publish request channel
    pub fn new(
        client: MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>,
        module: M,
        publisher_rx: Receiver<'a, CriticalSectionRawMutex, PublishRequest<'a>, OUTBOX_DEPTH>,
    ) -> Self {
        Self {
            client,
            module,
            publisher_rx,
        }
    }

    /// Run the MQTT runtime event loop (forever, unless an error occurs).
    pub async fn run(&mut self) -> Result<(), MqttError<T::Error>> {
        self.run_inner(None).await
    }

    /// Run the MQTT runtime event loop, but return `Ok(())` promptly when `abort` is set.
    ///
    /// 用于「配网成功切换网络」等场景主动中断当前会话、触发重连：`abort` 由外部
    /// （如另一个线程）置位，runtime 在每次 tick 循环顶部检测到即主动返回 `Ok(())`
    /// （视作「主动重连」而非错误断线，调用方可据此立即重连、不计入错误退避）。
    ///
    /// 必须在 `futures::executor::block_on` 单线程模型下可靠工作：本 runtime 的 tick 循环
    /// 由 `StdTcpTransport::recv_timeout`（阻塞读超时）驱动、每 ~500ms 真实推进，`abort`
    /// 检查点随之被可靠轮询——不依赖任何跨线程 waker 通知。
    pub async fn run_until(&mut self, abort: &AtomicBool) -> Result<(), MqttError<T::Error>> {
        self.run_inner(Some(abort)).await
    }

    async fn run_inner(
        &mut self,
        abort: Option<&AtomicBool>,
    ) -> Result<(), MqttError<T::Error>> {
        if let Some(last_will) = self.module.last_will()
            && !self.client.set_last_will(last_will)
        {
            return Err(MqttError::BufferTooSmall);
        }

        // Connect to the broker
        self.client.connect().await?;

        // Collect and subscribe to topics
        {
            let mut registry = TopicRegistry::<MAX_TOPICS>::new();
            self.module.register(&mut registry);

            // Subscribe to all registered topics
            for topic in registry.iter() {
                self.client.subscribe(topic, QoS::AtMostOnce).await?;
            }
        }

        // Create a reusable outbox for module callbacks
        let mut outbox: BufferedOutbox<OUTBOX_CAPACITY, OUTBOX_TOPIC_SIZE, OUTBOX_PAYLOAD_SIZE> =
            BufferedOutbox::new();

        // Call on_start for initial setup
        self.module.on_start(&mut outbox);
        self.drain_outbox(&mut outbox).await?;

        // Initial tick and set deadline for next tick
        let tick_interval = self.module.on_tick(&mut outbox);
        self.drain_outbox(&mut outbox).await?;
        let mut tick_deadline = Instant::now() + tick_interval;

        // Main event loop
        loop {
            // 配网触发重连：外部置位 `abort` 即主动中断当前会话、立即返回（由调用方重连）。
            // 置于循环顶部，确保每轮（~500ms）都能可靠检查到。
            if let Some(ab) = abort {
                if ab.load(Ordering::SeqCst) {
                    return Ok(());
                }
            }

            // First, check for incoming publish requests (non-blocking)
            if let Ok(req) = self.publisher_rx.try_receive() {
                self.client
                    .publish_with_retain(req.topic, req.payload, req.qos, req.retain)
                    .await?;
                continue;
            }

                // 把 tick 截止作为 recv 上限传给 poll()，不再依赖 embassy Timer。
                // poll() 内部按真实时钟判保活；tick 到点后在此处（poll 返回）统一处理。
                let now = Instant::now();
                let remaining = if now >= tick_deadline {
                    Duration::from_millis(0)
                } else {
                    tick_deadline - now
                };

                match self.client.poll(remaining).await {
                    Ok(Some(MqttEvent::Publish(msg))) => {
                        self.module.on_message(&msg);
                        if self.module.needs_immediate_publish() {
                            self.module.on_publish(&mut outbox);
                            self.drain_outbox(&mut outbox).await?;
                        }
                    }
                    Ok(None) => {
                        // 保活已发 / tick 唤醒 / 空闲，继续
                    }
                    Err(e) => return Err(e),
                }

                // poll 返回后检查 tick 是否到点（真实时钟）
                let now = Instant::now();
                if now >= tick_deadline {
                    let interval = self.module.on_tick(&mut outbox);
                    self.drain_outbox(&mut outbox).await?;
                    tick_deadline = now + interval;
                }
        }
    }

    /// Drain the outbox and publish all buffered messages.
    async fn drain_outbox(
        &mut self,
        outbox: &mut BufferedOutbox<OUTBOX_CAPACITY, OUTBOX_TOPIC_SIZE, OUTBOX_PAYLOAD_SIZE>,
    ) -> Result<(), MqttError<T::Error>> {
        for req in outbox.drain() {
            self.client
                .publish_with_retain(
                    req.topic.as_str(),
                    req.payload.as_slice(),
                    req.qos,
                    req.retain,
                )
                .await?;
        }
        outbox.clear();
        Ok(())
    }

    /// Get a reference to the underlying module.
    pub fn module(&self) -> &M {
        &self.module
    }

    /// Get a mutable reference to the underlying module.
    pub fn module_mut(&mut self) -> &mut M {
        &mut self.module
    }
}
