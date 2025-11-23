#wait_for_broker.py

#!/usr/bin/env python3
"""
Wait until the MQTT broker accepts a real MQTT CONNECT.
On success: replace this process with the command passed by Docker (CMD).
Exits 1 on timeout.
"""

import os
import sys
import time
import paho.mqtt.client as mqtt

HOST = os.environ.get("MQTT_BROKER_HOST", "mosquitto")
PORT = int(os.environ.get("MQTT_BROKER_PORT", 1883))
TIMEOUT = int(os.environ.get("WAIT_TIMEOUT", 120))   # seconds
SLEEP = float(os.environ.get("WAIT_SLEEP", 0.8))     # seconds between tries
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT", 5))  # socket/connect timeout

deadline = time.time() + TIMEOUT
print(f"Waiting for MQTT broker {HOST}:{PORT} (timeout {TIMEOUT}s)...", file=sys.stderr)

while time.time() < deadline:
    try:
        client = mqtt.Client()
        client.connect(HOST, PORT, keepalive=CONNECT_TIMEOUT)
        client.disconnect()
        print("Broker accepted MQTT connection", file=sys.stderr)
        # If Docker passed a command (CMD), exec it now.
        if len(sys.argv) > 1:
            cmd = sys.argv[1:]
            print("Executing:", " ".join(cmd), file=sys.stderr)
            os.execvp(cmd[0], cmd)
        # otherwise just exit success
        sys.exit(0)
    except Exception as e:
        print(f"connect attempt failed: {e}", file=sys.stderr)
        time.sleep(SLEEP)

print("Timed out waiting for MQTT broker", file=sys.stderr)
sys.exit(1)
