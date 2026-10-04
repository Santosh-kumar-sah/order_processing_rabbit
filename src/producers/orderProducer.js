const { getChannel } = require("../config/rabbitmq");

async function publishOrderCreated(order) {

    const channel = getChannel();

    const exchange = "orders.topic";

    await channel.assertExchange(exchange, "topic", {
        durable: true
    });

    const message = JSON.stringify(order);

    channel.publish(
        exchange,
        "order.created",
        Buffer.from(message),
        {
            persistent: true
        }
    );

    console.log("Order event published:", order.orderId);
}

module.exports = {
    publishOrderCreated
};