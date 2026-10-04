require("dotenv").config();

const { connectRabbitMQ } = require("../config/rabbitmq");

async function startWorker() {
    const channel = await connectRabbitMQ();

    const exchange = "orders.topic";

    const queue = "order.queue";

    const dlx = "orders.dlx";// Dead letter exchange
    const dlq = "order.dlq";// Dead letter queue

    await channel.assertExchange(exchange, "topic", {
        durable: true
    });

    // Dead letter exchange and queue setup
    await channel.assertExchange(dlx, "direct", {
        durable: true
    });

    await channel.assertQueue(dlq, {
        durable: true
    });

    await channel.bindQueue(dlq, dlx, "order.failed");

    // Main order queue setup with dead letter configuration
    await channel.assertQueue(queue, {
        durable: true,
        arguments: {
            "x-dead-letter-exchange": dlx,
            "x-dead-letter-routing-key": "order.failed"
        }
    });

    await channel.bindQueue(queue, exchange, "order.created");

    channel.prefetch(1);

    console.log("Order worker waiting...");

    channel.consume(queue, async (message) => {
        try {
            const order = JSON.parse(message.content.toString());

            console.log("Processing order:", order);

            await new Promise(resolve => setTimeout(resolve, 2000));

            // Failure simulation
            if (order.product === "fail") {
                throw new Error("Order processing failed");
            }

            console.log(`Order ${order.orderId} processed`);

            channel.ack(message);

        } catch (error) {
            console.error("Order processing failed:", error);

            channel.nack(message, false, false);
        }
    });
}

startWorker();