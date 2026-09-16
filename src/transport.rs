//! # MQTT Transport Abstraction
//!
//! This module defines the `MqttTransport` trait, which abstracts the underlying
//! communication channel (like TCP, UART, etc.), allowing the MQTT client to be
//! hardware and network-stack agnostic.
//!
//! With the Rust 2024 Edition, this trait uses native `async fn`, removing the
//! need for the `#[async_trait]` macro.

use crate::error::MqttError;

/// A placeholder error type used in contexts where the actual transport error is not known,
/// such as in the `EncodePacket` trait.
#[derive(Debug, Copy, Clone)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ErrorPlaceHolder;

/// A trait representing a transport for MQTT packets.
#[allow(async_fn_in_trait)]
pub trait MqttTransport {
    /// The error type returned by the transport.
    type Error: core::fmt::Debug;

    /// Sends a buffer of data over the transport.
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;

    /// Receives data from the transport into a buffer.
    ///
    /// Returns the number of bytes read.
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;

    /// 带超时的接收：至多阻塞 `timeout` 时间，超时且无数据时返回 `Ok(0)`（由调用方视为空闲）。
    ///
    /// std 构建（RV1106 Linux）下 `StdTcpTransport` 用 socket 读超时实现真实定时，
    /// 使保活/周期 tick 在 `futures::block_on` 下照常工作。此处默认实现退化为 `recv`
    /// （忽略 `timeout`），传输层可覆写为带真实超时的版本。
    async fn recv_timeout(&mut self, buf: &mut [u8], timeout: std::time::Duration) -> Result<usize, Self::Error> {
        let _ = timeout;
        self.recv(buf).await
    }
}

// Allow the placeholder to be treated as a transport error for generic contexts.
impl TransportError for ErrorPlaceHolder {}

/// A marker trait for transport-related errors.
pub trait TransportError: core::fmt::Debug {}

// Implement TransportError for MqttError so generic transports work with client methods
impl<T: core::fmt::Debug> TransportError for MqttError<T> {}
