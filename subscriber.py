#!/usr/bin/env python3
import json
import os
import socket
from datetime import datetime
import logging
import paho.mqtt.client as mqtt
from threading import Lock

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("subscriber")

# files
TEXT_LOG = "messages.txt"
JSON_LOG = "messages.json"
JSON_LOCK = Lock()

# MQTT settings
BROKER = "0.tcp.ap.ngrok.io"   # change to your broker if needed
PORT = 13052
TOPIC = "test/topic"
STATUS_TOPIC = f"{TOPIC}/status"
HEARTBEAT_TOPIC = f"{TOPIC}/heartbeat"
QOS = 1
# Generate unique client ID based on hostname and process ID
CLIENT_ID = f"Subscriber-{socket.gethostname()}-{os.getpid()}"

# -------------------------
# Helper: append object to messages.json (keeps array)
# -------------------------
def append_json_record(record: dict, filename: str = JSON_LOG):
    """
    Append a JSON object to a JSON array file.
    Creates the file with [record] if it doesn't exist.
    """
    with JSON_LOCK:
        if not os.path.exists(filename):
            with open(filename, "w", encoding="utf-8") as fh:
                json.dump([record], fh, ensure_ascii=False, indent=2)
            return

        try:
            with open(filename, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if not isinstance(data, list):
                    log.warning("%s exists but is not a JSON array. Backing up and starting new array.", filename)
                    os.rename(filename, filename + ".bak")
                    data = []
        except Exception as e:
            log.warning("Failed reading %s: %s. Backing up and starting new array.", filename, e)
            if os.path.exists(filename):
                os.rename(filename, filename + ".bak")
            data = []

        data.append(record)
        with open(filename, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)

# -------------------------
# JSON Parsing
# -------------------------
def try_parse_json_payload(payload_text: str):
    """Try multiple strategies to parse JSON from payload text."""
    # 1) direct parse
    try:
        return json.loads(payload_text)
    except Exception:
        pass

    # 2) unescape then parse
    try:
        unescaped = payload_text.encode("utf-8").decode("unicode_escape")
        return json.loads(unescaped)
    except Exception:
        pass

    # 3) replace escaped quotes
    try:
        cleaned = payload_text.replace("\\\"", "\"").replace("\\\\", "\\")
        return json.loads(cleaned)
    except Exception:
        pass

    raise ValueError("Not valid JSON")

# -------------------------
# Callbacks
# -------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Connected to MQTT Broker as {CLIENT_ID}!")
        
        # Subscribe to main topic with QoS=1
        client.subscribe((TOPIC, QOS))
        
        # Subscribe to heartbeat topic to respond when publisher is ready
        client.subscribe((HEARTBEAT_TOPIC, 0))
        
        # Announce our presence to the publisher
        log.info("Announcing presence to publisher...")
        client.publish(STATUS_TOPIC, CLIENT_ID, qos=1)
    else:
        print(f"❌ Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    # Handle heartbeat messages
    if msg.topic == HEARTBEAT_TOPIC:
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
            if payload == "ready":
                log.info("Received heartbeat from publisher, announcing presence...")
                client.publish(STATUS_TOPIC, CLIENT_ID, qos=1)
        except Exception as e:
            log.exception("Error handling heartbeat: %s", e)
        return
    
    # Handle data messages
    if msg.topic == TOPIC:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        topic = msg.topic
        try:
            message = msg.payload.decode("utf-8", errors="replace")
        except Exception:
            message = str(msg.payload)

        log_entry = f"[{timestamp}] Topic: {topic} | Message: {message}\n"
        print(f"📩 {log_entry.strip()}")

        # Append to the text log
        try:
            with open(TEXT_LOG, "a", encoding="utf-8") as f:
                f.write(log_entry)
        except Exception as e:
            log.exception("Failed to write text log: %s", e)

        # Try to parse JSON
        try:
            parsed = try_parse_json_payload(message)
            if isinstance(parsed, dict):
                record = parsed
            else:
                record = {"value": parsed}
        except Exception:
            record = {"raw": message}

        # Enrich record with metadata
        record_meta = {
            "_received_at": timestamp,
            "_topic": topic,
            "_client_id": CLIENT_ID
        }
        final_record = {**record_meta, **record}

        # Append to JSON file
        try:
            append_json_record(final_record)
        except Exception as e:
            log.exception("Failed to append JSON record: %s", e)

# -------------------------
# Main
# -------------------------
def main():
    log.info("Starting subscriber with ID: %s", CLIENT_ID)
    
    client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        log.info("Connecting to broker %s:%s", BROKER, PORT)
        client.connect(BROKER, PORT)
    except Exception as e:
        log.exception("Failed to connect to broker %s:%s - %s", BROKER, PORT, e)
        return

    # loop_forever processes incoming packets and sends acks
    client.loop_forever()

if __name__ == "__main__":
    main()