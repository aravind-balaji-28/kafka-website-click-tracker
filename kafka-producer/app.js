const express = require("express");
const cors = require("cors");
const { Kafka } = require("kafkajs");
const app = express();
app.use(cors());
app.use(express.json());
const kafka = new Kafka({
  clientId: "website-tracker",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();
const admin  = kafka.admin()
const connectProducer = async () => {
  await producer.connect();
  await admin.connect();    
  console.log("Kafka Producer Connected");
};
connectProducer();

app.get("/count", async (req, res) => {
  const offsets = await admin.fetchTopicOffsets("click-events");

  const totalCount = offsets.reduce(
    (sum, { high }) => sum + parseInt(high), 0
  );

  console.log("Fetched total count:", totalCount);
  res.json({ totalCount });   
});

app.post("/track", async (req, res) => {
  const event = req.body;
  console.time();
  await producer.send({
    topic: "click-events",
    messages: [
      {
        value: JSON.stringify(event),
      },
    ],
  });
  console.timeEnd();
  console.log("Event Produced:", event);
  res.send("Event Sent to Kafka");
});

app.listen(5001, () => {
  console.log("Server running on port 5001");
});
