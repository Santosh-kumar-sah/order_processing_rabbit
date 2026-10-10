const amqp = require("amqplib");

let connection;
let channel;

async function connectRabbitMQ() {

    connection = await amqp.connect(
        process.env.RABBITMQ_URL || "amqp://localhost:5672"
    );

    channel = await connection.createChannel();

    console.log("RabbitMQ connected");

    return channel;
}

function getChannel() {
    if (!channel) {
        throw new Error("RabbitMQ channel is not connected");
    }

    return channel;
}

module.exports = {
    connectRabbitMQ,
    getChannel
};