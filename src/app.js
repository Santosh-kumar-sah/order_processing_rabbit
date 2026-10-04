const express = require("express");

const orderRoutes = require("./routes/orderRoutes");

const app = express();

app.use(express.json());

app.use("/api/orders", orderRoutes);

module.exports = app;