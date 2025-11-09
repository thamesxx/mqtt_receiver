#!/usr/bin/env python3
import time
import json
import csv
import yaml
import logging
import sys
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("publisher")

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
# MQTT: robust publish that waits for confirmation
# ------------------------------
def create_client(cfg):
    broker = cfg.get("broker")
    port = int(cfg.get("port", 1883))
    client_id = cfg.get("client_id", f"publisher-{int(time.time())}")
    username = cfg.get("username") or None
    password = cfg.get("password") or None
    tls = bool(cfg.get("tls", False))
    keepalive = int(cfg.get("keepalive", 60))

    # Use modern constructor to avoid deprecated callback API warning
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    if username:
        client.username_pw_set(username, password or "")

    if tls:
        client.tls_set()           # system CA
        client.tls_insecure_set(False)

    # Optional callbacks for logging:
    def on_connect(c, userdata, flags, rc, properties=None):
        if rc == 0:
            log.info("Connected to MQTT Broker (broker=%s:%s)", broker, port)
        else:
            log.warning("Connect returned rc=%s", rc)

    def on_disconnect(c, userdata, rc):
        log.info("Disconnected (rc=%s)", rc)

    def on_publish(c, userdata, mid):
        log.debug("Published mid=%s", mid)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    # connect and start loop
    client.connect(broker, port, keepalive)
    client.loop_start()
    return client

def publish_records(client, topic, records, qos=1, retain=False, wait_timeout=10):
    sent = 0
    for rec in records:
        payload = json.dumps(rec, ensure_ascii=False)
        log.info("Publishing to %s: %s", topic, payload)
        info = client.publish(topic, payload, qos=qos, retain=retain)
        # info is an MQTTMessageInfo — wait for it to be sent if possible
        try:
            ok = info.wait_for_publish(timeout=wait_timeout)
            if ok:
                log.info("Publish confirmed (mid=%s)", info.mid)
                sent += 1
            else:
                log.warning("Publish not confirmed within %ss (mid=%s)", wait_timeout, info.mid)
        except Exception as e:
            log.exception("Exception while waiting for publish: %s", e)
        # safe small pause between messages if you like
        time.sleep(0.1)
    return sent

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

    # mqtt client
    client = create_client(mqtt_cfg)

    # wait briefly for connection to establish
    time.sleep(1.0)

    sent = publish_records(client, topic, processed, qos=qos, retain=retain)
    log.info("Published %d/%d", sent, len(processed))

    # give a short grace period to flush packets, then cleanly stop
    time.sleep(1.0)
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else "config.yml"
    main(cfg_file)
