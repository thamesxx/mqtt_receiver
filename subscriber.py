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
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("subscriber")

# Enable MQTT client debug logging
logging.getLogger("paho").setLevel(logging.DEBUG)

# files
TEXT_LOG = "messages.txt"
JSON_LOG = "messages.json"
JSON_LOCK = Lock()

# MQTT settings
BROKER = "0.tcp.ap.ngrok.io"   # change to your broker if needed
PORT = 17806
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
def on_connect(client, userdata, flags, reason_code, properties=None):
    """Updated callback for VERSION2 API"""
    if reason_code == 0 or str(reason_code) == "Success":
        log.info(f"✅ Connected to MQTT Broker as {CLIENT_ID}!")
        log.info(f"Connection flags: {flags}")
        
        # Subscribe to main topic with QoS=1
        result, mid = client.subscribe(TOPIC, QOS)
        log.info(f"Subscribed to {TOPIC} with result={result}, mid={mid}")
        
        # Subscribe to heartbeat topic to respond when publisher is ready
        result2, mid2 = client.subscribe(HEARTBEAT_TOPIC, 0)
        log.info(f"Subscribed to {HEARTBEAT_TOPIC} with result={result2}, mid={mid2}")
        
        # Announce our presence to the publisher
        log.info("Announcing presence to publisher...")
        result3 = client.publish(STATUS_TOPIC, CLIENT_ID, qos=1)
        log.info(f"Announced with result={result3}")
    else:
        log.error(f"❌ Failed to connect, reason code: {reason_code}")

def on_subscribe(client, userdata, mid, reason_codes, properties=None):
    """Callback when subscription is confirmed"""
    log.info(f"Subscription confirmed for mid={mid}, reason_codes={reason_codes}")

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """Callback when disconnected"""
    log.warning(f"Disconnected! Reason: {reason_code}")

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
    log.info("=" * 60)
    log.info("Starting subscriber with ID: %s", CLIENT_ID)
    log.info("Broker: %s:%s", BROKER, PORT)
    log.info("=" * 60)
    
    # Use callback_api_version to avoid deprecation warning
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID, 
        protocol=mqtt.MQTTv311,
        clean_session=True
    )

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect

    try:
        log.info("Attempting connection to broker %s:%s...", BROKER, PORT)
        client.connect(BROKER, PORT, keepalive=60)
        log.info("Connect call completed, starting loop...")
    except Exception as e:
        log.exception("Failed to connect to broker %s:%s - %s", BROKER, PORT, e)
        return

    # loop_forever processes incoming packets and sends acks
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        log.info("Shutting down gracefully...")
        client.disconnect()
        log.info("Disconnected")

if __name__ == "__main__":
    main()