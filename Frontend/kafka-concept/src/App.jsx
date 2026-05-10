import axios from "axios";
import { useEffect, useState } from "react";
import mqtt from "mqtt";

function App() {
  const [events, setEvents] = useState([]);
  const [totalCount, setTotalCount] = useState(0);

  useEffect(() => {
    const fetchInitialCount = async () => {
      const res = await axios.get("http://localhost:5001/count");
      console.log("Initial count from Kafka:", res.data.totalCount);
      setTotalCount(res.data.totalCount);   
    };
    fetchInitialCount();
  }, []);

  useEffect(() => {
    const client = mqtt.connect("ws://localhost:9001");

    client.on("connect", () => {
      console.log("Connected to MQTT broker");
      client.subscribe("analytics/click-events", (err) => {
        if (err) console.log("Subscribe error:", err);
        else console.log("Subscribed successfully");
      });
    });

    client.on("message", (topic, message) => {
      const data = JSON.parse(message.toString());
      console.log("Received:", data);

      setEvents((prev) => [data, ...prev]);
      setTotalCount(data.totalCount ?? 0);  
    });

    client.on("error", (err) => console.log("MQTT Error:", err));

    return () => client.end();
  }, []);

  const sendClick = async () => {
    const eventData = {
      user: "Aravind",
      page: "Home Page",
      button: "Subscribe",
      time: new Date().toISOString(),
    };
    await axios.post("http://localhost:5001/track", eventData);
    console.log("Event Sent");
  };

  return (
    <div>
      <h1>Kafka Click Tracker</h1>
      <button onClick={sendClick}>Subscribe</button>

      <h1>Kafka MQTT Dashboard</h1>
      <p>Total: {totalCount}</p> 
    </div>
  );
}

export default App;