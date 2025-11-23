#subscriber.py

#!/usr/bin/env python3
import json
import os
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
JSON_LOCK = Lock()  # simple in-process lock for safe writes

# MQTT settings
BROKER = "0.tcp.ap.ngrok.io"
PORT = 13052  # whatever port ngrok gives you
TOPIC = "test/topic"
QOS = 1                # subscribe with QoS 1 so subscriber will receive QoS1 messages and ack properly
CLIENT_ID = "PythonReceiver"

# -------------------------
# Helper: append object to messages.json (keeps array)
# -------------------------
def append_json_record(record: dict, filename: str = JSON_LOG):
    """
    Append a JSON object to a JSON array file.
    Creates the file with [record] if it doesn't exist.
    Uses a simple read/append/write strategy (fine for small/medium logs).
    """
    with JSON_LOCK:
        if not os.path.exists(filename):
            # create file with array containing the object
            with open(filename, "w", encoding="utf-8") as fh:
                json.dump([record], fh, ensure_ascii=False, indent=2)
            return

        # read the current file
        try:
            with open(filename, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if not isinstance(data, list):
                    # unexpected content: back it up and start fresh
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
# Callbacks
# -------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        # Subscribe with QoS=1
        client.subscribe((TOPIC, QOS))
    else:
        print(f"❌ Failed to connect, return code {rc}")

def try_parse_json_payload(payload_text: str):
    """
    Try multiple strategies to parse JSON from payload text.
    Returns a python object (dict/list) if parsed, else raises ValueError.
    """
    # 1) direct parse
    try:
        return json.loads(payload_text)
    except Exception:
        pass

    # 2) sometimes payloads come with escaped quotes like "{\"k\":\"v\"}"
    # attempt unescape then parse
    try:
        unescaped = payload_text.encode("utf-8").decode("unicode_escape")
        return json.loads(unescaped)
    except Exception:
        pass

    # 3) final attempt: replace single-escaped quotes / stray backslashes
    try:
        cleaned = payload_text.replace("\\\"", "\"").replace("\\\\", "\\")
        return json.loads(cleaned)
    except Exception:
        pass

    raise ValueError("Not valid JSON")

def on_message(client, userdata, msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    topic = msg.topic
    try:
        message = msg.payload.decode("utf-8", errors="replace")
    except Exception:
        message = str(msg.payload)

    log_entry = f"[{timestamp}] Topic: {topic} | Message: {message}\n"
    print(f"📩 {log_entry.strip()}")

    # Append to the text log (existing behaviour)
    try:
        with open(TEXT_LOG, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except Exception as e:
        log.exception("Failed to write text log: %s", e)

    # Try to parse JSON; if parsing fails, store it inside a wrapper object
    try:
        parsed = try_parse_json_payload(message)
        # if parsed is not a dict, wrap it so JSON log is an array of objects
        if isinstance(parsed, dict):
            record = parsed
        else:
            record = {"value": parsed}
    except Exception:
        # Not JSON -> try a simple heuristic: if message looks like key=value pairs, convert
        record = {"raw": message}
        # You could implement ad-hoc parsing here if needed

    # Enrich record with metadata (timestamp, topic, client info)
    record_meta = {
        "_received_at": timestamp,
        "_topic": topic
    }
    # merge: if record already has keys we preserve them; metadata in _ keys
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
    # Use modern constructor to avoid deprecation warning
    client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER, PORT)
    except Exception as e:
        log.exception("Failed to connect to broker %s:%s - %s", BROKER, PORT, e)
        return

    # loop_forever is OK here; it lets MQTT client process incoming packets and send acks
    client.loop_forever()

if __name__ == "__main__":
    main()
