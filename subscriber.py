import paho.mqtt.client as mqtt
from datetime import datetime

# --- Callback when connection is established ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        client.subscribe("test/topic")  # Subscribe to a topic
    else:
        print(f"❌ Failed to connect, return code {rc}")

# --- Callback when a message is received ---
def on_message(client, userdata, msg):
    message = msg.payload.decode()
    topic = msg.topic
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_entry = f"[{timestamp}] Topic: {topic} | Message: {message}\n"
    print(f"📩 {log_entry.strip()}")

    # Append message to file
    with open("messages.txt", "a", encoding="utf-8") as f:
        f.write(log_entry)

# --- Setup client ---
broker = "localhost"  # or your broker IP (e.g., "broker.emqx.io" or "test.mosquitto.org")
port = 1883

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "PythonReceiver")
client.on_connect = on_connect
client.on_message = on_message

# --- Connect and listen ---
client.connect(broker, port)
client.loop_forever()
