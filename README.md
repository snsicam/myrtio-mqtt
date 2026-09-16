# myrtio-mqtt

An async MQTT client for `std` (Linux) environments, built on the [Embassy](https://embassy.dev/) ecosystem's timers and synchronization primitives.

## Features

- **Async & `std`**: Runs on Linux/std platforms (e.g. RV1106) using `std::time`-driven `recv_timeout` keep-alive/tick.
- **No Allocator Required**: Uses `heapless` for fixed-size buffers and internal state management.
- **Transport Agnostic**: Works over TCP, UART, or any reliable stream-based channel via the `MqttTransport` trait. The application provides the transport (e.g. a std `TcpStream` wrapper).
- **MQTT v3.1.1 & v5**: Core support for v3.1.1 with optional v5 support via feature flags.
- **Modular Runtime**: High-level `MqttRuntime` for building applications using object-safe `MqttModule`s.

## Crate Requirements

- **Rust Edition 2024**: Uses native `async fn` in traits.
- **Rust Version**: 1.90 or newer.

## Usage Styles

### 1. Direct Client Usage
Use `MqttClient` directly for simple, low-level control.

```rust
use myrtio_mqtt::{MqttClient, MqttOptions, MqttEvent, QoS};
use embassy_time::Duration;

// `transport` is an application-provided `MqttTransport` (e.g. a std TcpStream wrapper).
let transport = MyTcpTransport::connect("broker.local:1883")?;
let options = MqttOptions::new("my-device-id");
// Client with space for 8 subscriptions and 1024-byte buffers
let mut client = MqttClient::<_, 8, 1024>::new(transport, options);

client.connect().await?;
client.subscribe("sensors/data", QoS::AtMostOnce).await?;

loop {
    // Poll handles keep-alives and incoming packets
    if let Ok(Some(MqttEvent::Publish(msg))) = client.poll(Duration::from_secs(1)).await {
        // Handle incoming message
        // msg.payload borrows from client.rx_buffer
    }
}
```

### 2. Runtime & Modules
Use `MqttRuntime` to compose multiple independent features (Home Assistant, Telemetry, etc.) into one event loop.

```rust
use myrtio_mqtt::runtime::{MqttModule, TopicCollector, PublishOutbox};
use myrtio_mqtt::packet::Publish;
use myrtio_mqtt::QoS;
use embassy_time::Duration;

struct TelemetryModule;

impl MqttModule for TelemetryModule {
    fn register(&self, collector: &mut dyn TopicCollector) {
        // Register topics for subscription
        collector.add("device/commands");
    }

    fn on_message(&mut self, msg: &Publish<'_>) {
        // Process incoming messages synchronously
    }

    fn on_tick(&mut self, outbox: &mut dyn PublishOutbox) -> Duration {
        // Periodic tasks: publish state
        outbox.publish("device/telemetry", b"{\"temp\": 22.5}", QoS::AtMostOnce);
        Duration::from_secs(30)
    }
}
```

## Key Concepts

- **Borrowed Payloads**: For efficiency, incoming `Publish` messages borrow their topic and payload directly from the client's internal receive buffer. They are only valid until the next call to `poll()` or until the module's `on_message` returns.
- **Object-Safe Design**: The `MqttModule` trait is object-safe (`dyn MqttModule`), allowing you to compose modules using `ModulePair` without complex generic parameters.
- **Outbox Pattern**: To keep modules object-safe and synchronous, they do not perform async I/O. Instead, they queue publish requests into a `PublishOutbox`. The `MqttRuntime` performs the actual async publishing after the module callback completes.

### Quick API Reference

| Module | Key Types |
|--------|-----------|
| **Root** | `MqttClient`, `MqttOptions`, `MqttEvent`, `QoS` |
| `transport` | `MqttTransport`, `TransportError` |
| `runtime` | `MqttRuntime`, `MqttModule`, `TopicCollector`, `PublishOutbox`, `PublisherHandle` |
