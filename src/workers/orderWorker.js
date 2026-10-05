require("dotenv").config();

const { connectRabbitMQ } = require("../config/rabbitmq");

const processedOrders = new Set();

async function startWorker() {
    const channel = await connectRabbitMQ();

    const exchange = "orders.topic";
    const queue = "order.queue";

    const retryExchange = "orders.retry";
    const retryQueue = "order.retry.queue";

    const dlx = "orders.dlx";
    const dlq = "order.dlq";

    const MAX_RETRIES = 3;

    // Main exchange
    await channel.assertExchange(exchange, "topic", {
        durable: true
    });

    // Retry exchange
    await channel.assertExchange(retryExchange, "direct", {
        durable: true
    });

    // Dead Letter Exchange
    await channel.assertExchange(dlx, "direct", {
        durable: true
    });

    // Main queue
    await channel.assertQueue(queue, {
        durable: true,
        arguments: {
            "x-dead-letter-exchange": dlx,
            "x-dead-letter-routing-key": "order.failed"
        }
    });

    // Retry queue
    await channel.assertQueue(retryQueue, {
        durable: true,
        arguments: {
            "x-message-ttl": 5000,

            "x-dead-letter-exchange": retryExchange,

            // FIXED
            "x-dead-letter-routing-key": "retry"
        }
    });

    // Dead Letter Queue
    await channel.assertQueue(dlq, {
        durable: true
    });

    // Main queue binding
    await channel.bindQueue(
        queue,
        exchange,
        "order.created"
    );

    // Retry queue binding
    await channel.bindQueue(
        retryQueue,
        retryExchange,
        "retry"
    );

    // DLQ binding
    await channel.bindQueue(
        dlq,
        dlx,
        "order.failed"
    );

    channel.prefetch(1);

    console.log("Order worker waiting...");

    channel.consume(queue, async (message) => {

        try {

            const order = JSON.parse(
                message.content.toString()
            );

            /*
             * Check if order was already processed
             */

            if (processedOrders.has(order.orderId)) {

                console.log(
                    `Order ${order.orderId} already processed`
                );

                channel.ack(message);

                return;
            }

            /*
             * Check redelivery
             */

            console.log(
                `Order ${order.orderId} | redelivered: ${message.fields.redelivered}`
            );

            /*
             * Retry count
             */

            const headers = message.properties.headers || {};

            const retryCount =
                headers["x-retry-count"] || 0;

            console.log(
                `Processing order ${order.orderId}, attempt: ${retryCount + 1}`
            );

            /*
             * Simulate processing
             */

            await new Promise(resolve =>
                setTimeout(resolve, 2000)
            );

            /*
             * Failure simulation
             */

            if (order.product === "fail") {
                throw new Error(
                    "Order processing failed"
                );
            }

            /*
             * Processing successful
             */

            console.log(
                `Order ${order.orderId} processed successfully`
            );

            /*
             * Mark order as processed
             */

            processedOrders.add(order.orderId);

            /*
             * ACK
             */

            channel.ack(message);

        } catch (error) {

            const headers =
                message.properties.headers || {};

            const retryCount =
                headers["x-retry-count"] || 0;

            console.error(
                `Order failed. Retry count: ${retryCount}`
            );

            /*
             * Retry
             */

            if (retryCount < MAX_RETRIES) {

                const nextRetryCount =
                    retryCount + 1;

                console.log(
                    `Sending order to retry queue. Retry: ${nextRetryCount}`
                );

                channel.publish(
                    retryExchange,
                    "retry",
                    message.content,
                    {
                        persistent: true,

                        headers: {
                            "x-retry-count":
                                nextRetryCount
                        }
                    }
                );

                channel.ack(message);

            } else {

                /*
                 * Maximum retries reached
                 */

                console.log(
                    `Maximum retries reached for order ${message.content.toString()}`
                );

                /*
                 * Send to DLQ through DLX
                 */

                channel.publish(
                    dlx,
                    "order.failed",
                    message.content,
                    {
                        persistent: true,

                        headers: {
                            "x-retry-count":
                                retryCount
                        }
                    }
                );

                channel.ack(message);
            }
        }
    });
}

startWorker();