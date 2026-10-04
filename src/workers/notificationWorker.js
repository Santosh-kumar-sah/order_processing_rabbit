require("dotenv").config();

const { connectRabbitMQ } = require("../config/rabbitmq");

async function startWorker() {
    const channel = await connectRabbitMQ();

    const exchange = "orders.topic";
    const queue = "notification.queue";

    await channel.assertExchange(exchange, "topic", {
        durable: true
    });

    await channel.assertQueue(queue, {
        durable: true
    });

    await channel.bindQueue(queue, exchange, "order.created");

    channel.prefetch(1);

    console.log("Notification worker waiting...");

    channel.consume(queue, async (message) => {
        try {
            const order = JSON.parse(message.content.toString());

            console.log("Sending notification for order:", order);

            await new Promise(resolve => setTimeout(resolve, 1000));

            console.log(
                `Notification sent for order ${order.orderId}`
            );

            channel.ack(message);

        } catch (error) {
            console.error("Notification processing failed:", error);

            channel.nack(message, false, true);
        }
    });
}

startWorker();