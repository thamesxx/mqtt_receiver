#!/usr/bin/env python3
import time
import json
import csv
import yaml
import logging
import sys
import paho.mqtt.client as mqtt
from threading import Lock

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("publisher")

# Track subscribers we've already sent data to
KNOWN_SUBSCRIBERS = set()
SUBSCRIBER_LOCK = Lock()

# ------------------------------
# Helpers to load YAML + data
# ------------------------------
def load_config(path="config.yml"):
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def load_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append(dict(r))
    return rows

def load_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        return [data]
    return data

# ------------------------------
# Simple auto conversion (small)
# ------------------------------
def auto_convert_record(rec: dict):
    out = {}
    for k, v in rec.items():
        if isinstance(v, str):
            s = v.strip()
            # try int
            try:
                if s != "" and "." not in s:
                    out[k] = int(s)
                    continue
            except:
                pass
            # try float
            try:
                if s != "":
                    out[k] = float(s)
                    continue
            except:
                pass
            # try bool
            low = s.lower()
            if low in ("true","t","yes","y","1","on"):
                out[k] = True; continue
            if low in ("false","f","no","n","0","off"):
                out[k] = False; continue
            # try json object/array
            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                try:
                    out[k] = json.loads(s)
                    continue
                except:
                    pass
            out[k] = s
        else:
            out[k] = v
    return out

# ------------------------------
# Publish records to topic
# ------------------------------
def publish_records(client, topic, records, qos=1, retain=False):
    sent = 0
    infos = []
    
    for rec in records:
        payload = json.dumps(rec, ensure_ascii=False)
        log.info("Publishing to %s: %s", topic, payload)
        info = client.publish(topic, payload, qos=qos, retain=retain)
        infos.append(info)
        time.sleep(0.1)
    
    # Wait for messages to be sent
    log.info("Waiting for messages to be sent...")
    time.sleep(2.0)
    
    # Check which ones were sent
    for info in infos:
        if info.is_published():
            sent += 1
        else:
            log.warning("Message mid=%s not confirmed", info.mid)
    
    return sent

# ------------------------------
# MQTT Callbacks
# ------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("Connected to MQTT Broker")
    else:
        log.warning("Connect returned rc=%s", rc)

def on_disconnect(client, userdata, rc):
    log.info("Disconnected (rc=%s)", rc)

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    """Called when a client subscribes to our topic - MQTT v5 feature"""
    log.debug("Subscribe event detected (mid=%s)", mid)

def on_message(client, userdata, msg):
    """
    This won't be called unless we subscribe to a topic ourselves.
    We're using the $SYS topic to detect subscriber activity.
    """
    pass

# ------------------------------
# Monitor for new subscribers
# ------------------------------
def monitor_subscribers(client, topic, records, qos, retain):
    """
    Monitors $SYS/broker/clients/connected and client IDs to detect new subscribers.
    Note: This requires broker support for $SYS topics (Mosquitto has this).
    
    Alternative approach: We'll monitor subscription events by checking
    the broker logs or using MQTT v5 subscription identifiers.
    
    For simplicity, we'll use a polling approach with a status topic.
    """
    log.info("Starting subscriber monitoring...")
    log.info("Waiting for new subscribers to connect to topic: %s", topic)
    log.info("Known subscribers will not receive duplicate data")
    
    # Subscribe to a special status topic where subscribers announce themselves
    status_topic = f"{topic}/status"
    client.subscribe(status_topic, qos=1)
    log.info("Listening for subscriber announcements on: %s", status_topic)
    log.info("Note: Subscribers should publish their client_id to this topic when they connect")
    
    # Also set up a callback for the main topic to detect subscriptions
    def on_message_handler(client, userdata, msg):
        if msg.topic == status_topic:
            try:
                subscriber_id = msg.payload.decode('utf-8').strip()
                log.info("Received status message from: %s", subscriber_id)
                
                with SUBSCRIBER_LOCK:
                    if subscriber_id not in KNOWN_SUBSCRIBERS:
                        log.info("🆕 New subscriber detected: %s", subscriber_id)
                        KNOWN_SUBSCRIBERS.add(subscriber_id)
                        
                        # Publish data to the new subscriber
                        log.info("Publishing %d records for new subscriber: %s", len(records), subscriber_id)
                        sent = publish_records(client, topic, records, qos, retain)
                        log.info("✅ Sent %d/%d records to %s", sent, len(records), subscriber_id)
                    else:
                        log.info("⏭️  Subscriber %s already known, skipping publish", subscriber_id)
            except Exception as e:
                log.exception("Error processing status message: %s", e)
    
    client.on_message = on_message_handler
    
    # Keep the connection alive
    log.info("Publisher is now live and monitoring for new subscribers...")
    log.info("Press Ctrl+C to stop")
    
    try:
        # Keep running forever
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")

# ------------------------------
# Alternative: Simple periodic check approach
# ------------------------------
def monitor_with_heartbeat(client, topic, records, qos, retain):
    """
    Simpler approach: Periodically publish a heartbeat.
    Subscribers respond with their ID, triggering data send.
    """
    status_topic = f"{topic}/status"
    heartbeat_topic = f"{topic}/heartbeat"
    
    client.subscribe(status_topic, qos=1)
    
    def on_message_handler(client, userdata, msg):
        if msg.topic == status_topic:
            try:
                subscriber_id = msg.payload.decode('utf-8').strip()
                
                with SUBSCRIBER_LOCK:
                    if subscriber_id not in KNOWN_SUBSCRIBERS:
                        log.info("🆕 New subscriber: %s", subscriber_id)
                        KNOWN_SUBSCRIBERS.add(subscriber_id)
                        
                        sent = publish_records(client, topic, records, qos, retain)
                        log.info("✅ Sent %d records to %s", sent, subscriber_id)
                    else:
                        log.debug("Known subscriber: %s", subscriber_id)
            except Exception as e:
                log.exception("Error: %s", e)
    
    client.on_message = on_message_handler
    
    log.info("Publisher live - sending heartbeats every 10s")
    log.info("Subscribers should respond to %s with their client_id", heartbeat_topic)
    
    try:
        heartbeat_count = 0
        while True:
            # Send heartbeat to signal we're ready
            client.publish(heartbeat_topic, "ready", qos=0)
            heartbeat_count += 1
            if heartbeat_count % 6 == 0:  # Every minute
                log.info("Still monitoring... (known subscribers: %d)", len(KNOWN_SUBSCRIBERS))
            time.sleep(10)
    except KeyboardInterrupt:
        log.info("Shutting down...")

# ------------------------------
# main
# ------------------------------
def main(cfg_path="config.yml"):
    cfg = load_config(cfg_path)
    source = cfg.get("source", {})
    src_type = source.get("type", "csv").lower()
    path = source.get("path")
    if not path:
        log.error("source.path missing in config")
        sys.exit(1)

    # mqtt config
    mqtt_cfg = {
        "broker": cfg.get("mqtt", {}).get("broker"),
        "port": cfg.get("mqtt", {}).get("port", 1883),
        "username": cfg.get("mqtt", {}).get("username"),
        "password": cfg.get("mqtt", {}).get("password"),
        "tls": cfg.get("mqtt", {}).get("tls", False),
        "client_id": cfg.get("mqtt", {}).get("client_id"),
        "keepalive": cfg.get("mqtt", {}).get("keepalive", 60)
    }
    topic = cfg.get("mqtt", {}).get("topic", "test/topic")
    qos = int(cfg.get("mqtt", {}).get("qos", 1))
    retain = bool(cfg.get("mqtt", {}).get("retain", False))

    log.info("Loading data from %s (%s)", path, src_type)
    if src_type == "csv":
        raw = load_csv(path)
    elif src_type == "json":
        raw = load_json(path)
    else:
        log.error("Unsupported source.type: %s", src_type)
        sys.exit(1)

    # validate / convert
    processed = [auto_convert_record(r) for r in raw]
    log.info("Prepared %d records", len(processed))
    if not processed:
        log.warning("No records to publish")
        return

    # Create MQTT client
    broker = mqtt_cfg.get("broker")
    port = int(mqtt_cfg.get("port", 1883))
    client_id = mqtt_cfg.get("client_id", f"publisher-{int(time.time())}")
    username = mqtt_cfg.get("username") or None
    password = mqtt_cfg.get("password") or None
    tls = bool(mqtt_cfg.get("tls", False))
    keepalive = int(mqtt_cfg.get("keepalive", 60))

    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    if username:
        client.username_pw_set(username, password or "")

    if tls:
        client.tls_set()
        client.tls_insecure_set(False)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Connect with retry logic
    MAX_CONNECT_RETRIES = 12
    CONNECT_RETRY_DELAY = 1.0

    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            client.connect(broker, port, keepalive)
            break
        except Exception as e:
            log.warning("Connect attempt %d/%d failed: %s", attempt, MAX_CONNECT_RETRIES, e)
            if attempt == MAX_CONNECT_RETRIES:
                log.exception("Max connect attempts exceeded")
                raise
            time.sleep(CONNECT_RETRY_DELAY)

    # Start the network loop in background
    client.loop_start()
    time.sleep(1.0)  # Wait for connection to establish

    # Start monitoring for new subscribers
    # Choose one of the two approaches:
    # monitor_subscribers(client, topic, processed, qos, retain)  # Passive monitoring
    monitor_with_heartbeat(client, topic, processed, qos, retain)  # Active heartbeat

    # Cleanup
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else "config.yml"
    main(cfg_file)