const { Kafka } = require("kafkajs");
const mqtt = require("mqtt");

const kafka = new Kafka({
  clientId: "analytics-engine",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "click-group-live" });
const admin = kafka.admin();
const mqttClient = mqtt.connect("mqtt://localhost:1883");

async function getTotalCountFromOffsets(topic) {
  const offsets = await admin.fetchTopicOffsets(topic); 

  offsets.forEach(({ partition, high, low }) => {
    console.log(`Partition: ${partition} | Low: ${low} | High: ${high}`);
  });

  const totalCount = offsets.reduce((sum, { high }) => sum + parseInt(high), 0);
  console.log(`Total Messages in Topic: ${totalCount}`);

  return { offsets, totalCount };
}

mqttClient.on("connect", async () => {
  console.log("MQTT Connected");

  const { offsets, totalCount } = await getTotalCountFromOffsets("click-events");
  console.log("Initial Total Count:", totalCount);

  await admin.connect();
  console.log("Kafka Admin Connected");

  await consumer.connect()
  
  console.log("Kafka Consumer Connected");

  await consumer.subscribe({
    topic: "click-events",
    fromBeginning: false,
  });
  console.log("Kafka Subscribed");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log("Received Data:", data);

      const { totalCount: updatedCount } = await getTotalCountFromOffsets(topic);

      const payload = {
        totalCount: updatedCount, 
      };

      console.log("Publishing to MQTT:", payload);

      mqttClient.publish(
        "analytics/click-events",
        JSON.stringify(payload),
        (err) => {
          if (err) {
            console.log("Publish error:", err);
          } else {
            console.log("Published successfully | Total Count:", updatedCount);
          }
        }
      );
    },
  });
});
  
mqttClient.on("error", (err) => {
  console.log("MQTT Error:", err);
});