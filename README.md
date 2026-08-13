# mqtt_receiver

A containerised MQTT publish/subscribe pipeline. A publisher reads records from a CSV or JSON source, validates and type-coerces each one against a declared schema, and publishes them to a Mosquitto broker. A subscriber consumes the topic and durably appends every message to both a plain-text log and a JSON array.

Built while evaluating message brokers for a real-time machine-telemetry system — this was the MQTT/Mosquitto arm of that comparison. The RabbitMQ arm lives in [rabbitinflux](https://github.com/thamesxx/rabbitinflux), and the design that shipped is in [SmartAI_System_Final](https://github.com/thamesxx/SmartAI_System_Final).

---

## What's interesting here

**Schema-driven ingestion.** The publisher doesn't just dump strings onto a topic. `config.yml` declares each field's type (`int`, `float`, `bool`, `json`), whether it's required, and a default. Records are coerced and validated before publishing, so the subscriber gets typed JSON rather than CSV text. Omit the schema entirely and auto-conversion is attempted per field.

**Startup ordering that actually works.** Container orchestration usually fails first at "the app started before the broker was ready." A Compose health check polls port 1883, and `wait_for_broker.py` blocks the publisher until the broker answers — with a configurable timeout, poll interval, and connect timeout.

**Crash-safe JSON logging.** The subscriber appends to a JSON array on disk under a thread lock. If the file is corrupt or isn't an array, it's backed up to `.bak` and a fresh array is started rather than losing the run.

**Multiple parse strategies.** Payloads are parsed through a fallback chain, so malformed or double-encoded JSON from a misbehaving publisher doesn't take the consumer down.

---

## Architecture

```
  data.csv / data.json
          |
          v
   [ publisher.py ]  --- schema validation + type coercion (config.yml)
          |
          | MQTT publish  ·  topic: test/topic  ·  QoS 1  ·  retain
          v
   [ Mosquitto 2.0 ]  --- eclipse-mosquitto, ports 1883 / 9001
          |
          | subscribe
          v
  [ subscriber.py ]  ---> messages.txt   (human-readable log)
                     ---> messages.json  (append-only JSON array)
```

Two side topics carry operational signal: `test/topic/status` and `test/topic/heartbeat`.

---

## Quick start

**Requirements:** Docker and Docker Compose.

```bash
docker compose up --build
```

That brings up Mosquitto, waits for the broker to pass its health check, then starts the publisher. `data/` and `config.yml` are mounted read-only into the publisher container.

Run the subscriber against the broker:

```bash
pip install -r requirements.txt
python subscriber.py
```

To run without Docker, start a local Mosquitto, then:

```bash
python publisher.py
```

---

## Configuration

Everything the publisher needs is in `config.yml`:

```yaml
source:
  type: csv                  # csv | json
  path: ./data/data.csv

mqtt:
  broker: "mosquitto"        # hostname or IP
  port: 1883
  username: ""
  password: ""
  tls: false
  topic: "test/topic"
  qos: 1                     # 0 | 1 | 2
  retain: true
  client_id: "publisher-01"
  keepalive: 60

schema:                      # optional — omit for auto-conversion
  id:
    type: int
    required: true
  temp:
    type: float
    required: true
  humidity:
    type: float
    required: false
    default: 0.0
  active:
    type: bool
    required: false
    default: false
  metadata:
    type: json               # dict or list
    required: false
    default: {}
```

Supported schema types: `int`, `float`, `bool`, `str`, `json`.

### Publisher container environment

| Variable | Default | Purpose |
|---|---|---|
| `MQTT_BROKER_HOST` | `mosquitto` | Broker hostname |
| `MQTT_BROKER_PORT` | `1883` | Broker port |
| `WAIT_TIMEOUT` | `120` | Seconds to wait for the broker before giving up |
| `WAIT_SLEEP` | `0.8` | Seconds between readiness polls |
| `CONNECT_TIMEOUT` | `5` | Per-attempt connect timeout |

### Subscriber

Broker host, port, topic, and QoS are set at the top of `subscriber.py`. The client ID is generated from hostname and PID, so several subscribers can run concurrently without ID collisions kicking each other off the broker.

---

## Ports

| Port | Service |
|---|---|
| 1883 | MQTT (TCP) |
| 9001 | MQTT over WebSockets |

---

## Repository layout

```
publisher.py           Reads source data, validates against schema, publishes
subscriber.py          Subscribes, parses, and logs to text + JSON
wait_for_broker.py     Blocks until the broker accepts connections
wait_for_broker.sh     Shell equivalent for the container entrypoint
config.yml             Source, MQTT connection, and field schema
docker-compose.yml     Mosquitto + publisher, with health-check gating
Dockerfile             Publisher image
mosquitto/config/      Broker configuration
requirements.txt       PyYAML, paho-mqtt
```

---

## Troubleshooting

**Publisher exits immediately** — the broker wasn't reachable within `WAIT_TIMEOUT`. Check `docker compose logs mosquitto` and confirm the health check is passing.

**Subscriber connects then drops** — two clients are using the same client ID. The generated `Subscriber-{hostname}-{pid}` ID prevents this; if you hardcoded one, make it unique.

**Messages arrive as strings** — the field isn't declared in `schema`, and auto-conversion couldn't infer a type. Declare it explicitly.

**Nothing on the topic after restart** — `retain: true` only retains the last message per topic. For history, read `messages.json`.

---

## License

MIT
