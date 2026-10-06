# RabbitMQ Order System

A Node.js order-processing service that publishes order events to RabbitMQ and processes them asynchronously with dedicated workers.

## Features

- Express API for creating orders
- RabbitMQ topic exchange for order events
- Separate workers for order processing, notifications, and email handling
- Retry handling for failed orders
- Dead-letter queue after the maximum retry count is reached

## Requirements

- Node.js 18 or later
- Docker and Docker Compose

## Setup

1. Install dependencies:

   ```bash
   npm install
   ```

2. Create a `.env` file in the project root:

   ```env
   PORT=5000
   RABBITMQ_URL=amqp://localhost:5672
   ```

3. Start RabbitMQ:

   ```bash
   docker compose up -d
   ```

   The RabbitMQ management dashboard is available at `http://localhost:15672`.

## Run the API

```bash
npm start
```

The API listens on port `5000` by default.

## Run the workers

Start each worker in a separate terminal:

```bash
node src/workers/orderWorker.js
node src/workers/notificationWorker.js
node src/workers/emailWorker.js
```

## API

### Create an order

```http
POST /api/orders
Content-Type: application/json
```

Request body:

```json
{
  "product": "keyboard",
  "quantity": 1
}
```

Example using `curl`:

```bash
curl -X POST http://localhost:5000/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"product\":\"keyboard\",\"quantity\":1}"
```

The API publishes the order to the `orders.topic` exchange with the `order.created` routing key.

## Retry behavior

The order worker retries failed orders up to three times. Orders that continue to fail are published to the dead-letter exchange and placed in the `order.dlq` queue. To simulate a processing failure, create an order with `"product": "fail"`.
