import paho.mqtt.client as mqtt

# --- Callback when connection is established ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        client.subscribe("test/topic")  # Subscribe to a topic
    else:
        print(f"❌ Failed to connect, return code {rc}")

# --- Callback when a message is received ---
def on_message(client, userdata, msg):
    print(f"📩 Received message from topic {msg.topic}: {msg.payload.decode()}")

# --- Setup client ---
broker = "localhost"  # or your broker IP (e.g., "broker.emqx.io" or "test.mosquitto.org")
port = 1883

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "PythonReceiver")
client.on_connect = on_connect
client.on_message = on_message

# --- Connect and listen ---
client.connect(broker, port)
client.loop_forever()