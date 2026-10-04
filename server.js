require("dotenv").config();

const app = require("./src/app");
const { connectRabbitMQ } = require("./src/config/rabbitmq");

const PORT = process.env.PORT || 5000;

async function startServer() {

    await connectRabbitMQ();

    app.listen(PORT, () => {
        console.log(`Server running on port ${PORT}`);
    });
}

startServer();