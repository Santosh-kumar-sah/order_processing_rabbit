require("dotenv").config();

const {
    connectRabbitMQ
} = require("../config/rabbitmq");

async function startWorker() {

    const channel = await connectRabbitMQ();

    const exchange = "orders.topic";
    const queue = "order.queue";

    await channel.assertExchange(exchange, "topic", {
        durable: true
    });

    await channel.assertQueue(queue, {
        durable: true
    });

    await channel.bindQueue(
        queue,
        exchange,
        "order.*"
    );

    channel.prefetch(1);

    console.log("Order worker waiting...");

    channel.consume(queue, async (message) => {

        try {

            const order = JSON.parse(
                message.content.toString()
            );

            console.log(
                "Processing order:",
                order
            );

            await new Promise(resolve =>
                setTimeout(resolve, 2000)
            );

            console.log(
                `Order ${order.orderId} processed`
            );

            channel.ack(message);

        } catch (error) {

            console.error(
                "Order processing failed:",
                error
            );

            channel.nack(
                message,
                false,
                true
            );
        }
    });
}

startWorker();