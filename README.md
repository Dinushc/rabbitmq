# rabbitmq-client

[![CI](https://github.com/dinushc/rabbitmq/actions/workflows/ci.yml/badge.svg)](https://github.com/dinushc/rabbitmq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/dinushc/rabbitmq.svg)](https://pkg.go.dev/github.com/dinushc/rabbitmq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Небольшая библиотека для RabbitMQ на Go: подключение с автопереподключением, publisher confirms, consumer с конкурентной обработкой, DLQ и готовые контракты событий.

Библиотека собрана из первой версии `proto-lib/rabbit`, но без найденных там багов:
- убран двойной `RUnlock`, который приводил к panic при ошибке объявления exchange;
- publisher и каждый consumer получают собственный AMQP channel, а не делят один общий;
- отслеживается закрытие channel, а не только connection;
- DLQ получил корректный `x-dead-letter-routing-key` — иначе dead-letter сообщения не попадали в DLQ;
- добавлены контексты, таймауты, jitter backoff, graceful shutdown и тесты.

## Установка

```bash
go get github.com/dinushc/rabbitmq@v0.1.0
```

Если библиотека будет публиковаться под другим доменом, измените `module` в `go.mod` и импорты. Пакеты от этого не зависят.

## Быстрый старт

### Publisher

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/dinushc/rabbitmq/client"
)

func main() {
	conn, err := client.New(client.Config{
		URL:  "amqp://guest:guest@localhost:5672/",
		Name: "my-service",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	pub := client.NewPublisher(conn, client.DefaultPublisherConfig())
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pub.PublishJSON(ctx, "account.events", "bet.placed", map[string]any{
		"transactionId": "8f14e45f-ea6f-4f2f-8f3a-1c2d3e4f5a6b",
		"amount":        "10.50",
	}); err != nil {
		log.Fatal(err)
	}
}
```

### Consumer

```go
package main

import (
	"context"
	"log"

	"github.com/dinushc/rabbitmq/client"
)

func main() {
	conn, err := client.New(client.Config{
		URL:  "amqp://guest:guest@localhost:5672/",
		Name: "my-service",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	consumer := client.NewConsumer(conn, "my-service")
	defer consumer.Close()

	err = consumer.SubscribeWithOpts(
		"my-service.account.bet_placed",
		"account.events",
		"bet.placed",
		func(ctx context.Context, msg client.Message) error {
			log.Printf("received %s: %s", msg.RoutingKey, msg.Body)
			return nil
		},
		client.DefaultSubscribeOptions(),
	)
	if err != nil {
		log.Fatal(err)
	}

	select {}
}
```

## Что гарантирует библиотека

- **Publisher confirms.** `Publish` ждёт подтверждения брокера и возвращает ошибку при nack или таймауте.
- **Автопереподключение.** `Connection` переподключается с экспоненциальным backoff и jitter. `Channel(ctx)` ждёт готовности соединения.
- **Автовосстановление подписок.** Consumer пересоздаёт channel, объявляет topology и запускает `Consume` заново.
- **Собственные channel'ы.** Publisher и каждая подписка работают на отдельных AMQP channel'ах.
- **DLQ.** По умолчанию создаётся `<queue>.dlq.exchange` и `<queue>.dlq`; ошибка handler'а отправляет сообщение в DLQ.
- **Конкурентная обработка.** `Concurrency` ограничивает число одновременных handler'ов, `Prefetch` — число unacked сообщений.
- **Graceful shutdown.** `Consumer.CloseContext` останавливает подписки и ждёт завершения handler'ов.

Семантика доставки — **at-least-once**. При падении соединения, таймауте подтверждения или перезапуске сообщение может быть обработано повторно. Обработчики должны быть идемпотентными.

## Конфигурация

### Connection

| Поле | По умолчанию | Назначение |
|------|--------------|------------|
| `URL` | — | AMQP URL |
| `Name` | `rabbitmq-client` | Имя подключения в RabbitMQ |
| `Heartbeat` | `10s` | AMQP heartbeat |
| `DialTimeout` | `10s` | Таймаут dial и reconnect |
| `InitialBackoff` | `500ms` | Первая задержка reconnect |
| `MaxBackoff` | `30s` | Максимальная задержка |
| `BackoffFactor` | `2` | Множитель задержки |
| `Jitter` | `0.2` | Случайное отклонение задержки |
| `TLSConfig` | `nil` | TLS для `amqps://` |
| `Logger` | `slog.Default()` | Логгер |

### Publisher

| Поле | По умолчанию | Назначение |
|------|--------------|------------|
| `PublishTimeout` | `5s` | Ожидание confirm |
| `MaxRetries` | `5` | Всего попыток публикации |
| `RetryDelay` | `100ms` | Первая задержка retry |
| `MaxRetryDelay` | `2s` | Максимальная задержка |
| `RetryFactor` | `2` | Множитель retry |
| `RetryJitter` | `0.2` | Jitter retry |
| `ExchangeType` | `topic` | Тип exchange по умолчанию |
| `Persistent` | `true` | `delivery_mode=2` |
| `Transient` | `false` | Отключает persistent и имеет приоритет над `Persistent` |
| `RetryOnTimeout` | `true` | Повторять publish при таймауте confirm |
| `DisableTimeoutRetries` | `false` | Отключает повторы после таймаута confirm |

`PublishOptions` позволяет переопределить exchange type, mandatory/immediate, content type, headers, message ID, correlation ID и таймаут для конкретного сообщения.

Частично заполненные конфиги наследуют остальные значения из `DefaultPublisherConfig()` и `DefaultSubscribeOptions()`. Для отключения поведения используйте явные поля `Transient`, `DisableTimeoutRetries`, `NonDurableExchange`, `NonDurableQueue`.

### Consumer

| Поле | По умолчанию | Назначение |
|------|--------------|------------|
| `ExchangeType` | `topic` | Тип exchange |
| `NonDurableExchange` | `false` | Объявить non-durable exchange |
| `NonDurableQueue` | `false` | Объявить non-durable queue |
| `Prefetch` | `10` | Unacked лимит |
| `Concurrency` | `10` | Параллельные handler'ы |
| `HandlerTimeout` | `30s` | Таймаут handler'а; отрицательное значение отключает |
| `RequeueOnError` | `false` | Requeue вместо DLQ |
| `DisableDLQ` | `false` | Отключить DLQ |
| `DLQ` | автогенерация имён | Настройка DLQ exchange/queue/routing key |

Handler может управлять результатом через sentinel-ошибки:

```go
return client.ErrRequeue // Nack с requeue
return client.ErrDiscard // Ack и удалить сообщение
return fmt.Errorf("...: %w", client.ErrRequeue)
```

## Контракты

В `contracts/` лежат готовые события из первой версии:
- `contracts/account` — баланс, ставки, выигрыши, депозиты, выводы, бонусы;
- `contracts/payment` — депозиты, выводы, chargeback;
- `contracts/vip` — изменение VIP-уровня и начисление поинтов.

Контракты не обязательны для использования клиента. JSON-поля приведены к `camelCase`, как в актуальных сервисах.

## Отличия от первой версии

Сохранены совместимые имена:
- `client.NewRabbitMQ`, `client.NewRabbitMQWithConfig`, `client.Config`;
- `client.NewConsumer`, `client.Consumer.Subscribe`, `SubscribeWithOpts`;
- `client.NewPublisher`, `client.DefaultPublisherConfig`, `client.Publish`;
- `client.Handler`, `client.Message`, `client.SubscribeOptions`.

Основные изменения:
- `Connection.Channel(ctx)` возвращает новый выделенный channel; `NewChannel()` оставлен для совместимости.
- `Publisher` и `Consumer` больше не используют общий channel.
- `Close` у consumer/publisher возвращает ошибку, но старый вызов `defer consumer.Close()` продолжает компилироваться.
- Добавлены `PublishJSON`, `PublishOptions`, `CloseContext`, `ErrRequeue`, `ErrDiscard`.
- Исправлен DLQ: main queue получает `x-dead-letter-routing-key`, DLQ bind использует тот же ключ.

## Тестирование

```bash
go test ./...
go test -race ./...
```

Интеграционные тесты проверяют публикацию, получение и маршрутизацию в DLQ. Они запускаются только при наличии RabbitMQ:

```bash
RABBITMQ_URL='amqp://guest:guest@localhost:5672/' go test ./client -run Integration -v
```

Юнит-тесты не требуют брокера.

## CI и релизы

GitHub Actions workflow `.github/workflows/ci.yml` запускает форматирование, `go mod tidy`, build, vet и race-тесты с RabbitMQ service. Пуш тега `v*` запускает `.github/workflows/release.yml`, который создаёт GitHub Release.

Текущая версия — `v0.1.0` (см. `VERSION` и `CHANGELOG.md`). Проект следует semver: до `v1.0.0` API может меняться, после — только обратно совместимо.

## Лицензия

MIT. См. [LICENSE](LICENSE).

## Структура

```
client/          подключение, publisher, consumer, конфигурация
contracts/       события account/payment/vip
examples/        примеры publisher и consumer
.github/         CI, release и dependabot
```
