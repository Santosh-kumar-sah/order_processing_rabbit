const amqp = require("amqplib");

let connection;
let channel;

async function connectRabbitMQ() {

    connection = await amqp.connect(process.env.RABBITMQ_URL);

    channel = await connection.createChannel();

    console.log("RabbitMQ connected");

    return channel;
}

function getChannel() {
    return channel;
}

module.exports = {
    connectRabbitMQ,
    getChannel
};