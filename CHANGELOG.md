# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-25

### Added

- `client.Connection` with automatic reconnection, exponential backoff, jitter
  and context-aware channel creation.
- `client.Publisher` with publisher confirms, per-message options, retries,
  exchange declaration cache and JSON helper.
- `client.Consumer` with per-subscription channels, QoS, configurable
  concurrency, handler timeouts, panic recovery and automatic resubscription.
- Dead-letter queue topology with explicit dead-letter routing keys.
- `contracts/account`, `contracts/payment` and `contracts/vip` event contracts.
- Compatibility aliases and constructors: `NewRabbitMQ`,
  `NewRabbitMQWithConfig`, `NewConsumer`, `NewPublisher`.
- Unit tests, RabbitMQ integration tests, examples, CI workflow and release
  workflow.

### Fixed

- Removed a double `RUnlock` that panicked when exchange declaration failed.
- Publisher and consumers no longer share one AMQP channel.
- Channel-level failures are detected and recovered, not only connection
  failures.
- Dead-lettered messages are routed to the DLQ reliably.
- Stale channel notifications and leaked old connections are handled safely.

### Changed

- Logging uses `log/slog` and is configurable.
- Added `Transient`, `DisableTimeoutRetries`, `NonDurableExchange` and
  `NonDurableQueue` for explicit opt-out of default behavior.

[Unreleased]: https://github.com/dinushc/rabbitmq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/dinushc/rabbitmq/releases/tag/v0.1.0
