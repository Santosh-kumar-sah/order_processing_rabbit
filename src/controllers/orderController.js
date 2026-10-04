const { publishOrderCreated } = require("../producers/orderProducer");

async function createOrder(req, res) {

    try {

        const { product, quantity } = req.body;

        const order = {
            orderId: Date.now(),
            product,
            quantity,
            status: "created"
        };

        await publishOrderCreated(order);

        res.status(201).json({
            message: "Order created",
            order
        });

    } catch (error) {

        console.error(error);

        res.status(500).json({
            message: "Failed to create order"
        });
    }
}

module.exports = {
    createOrder
};