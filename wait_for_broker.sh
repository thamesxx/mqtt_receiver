#!/usr/bin/env bash
# usage: environment variables MQTT_BROKER_HOST and MQTT_BROKER_PORT should be set

: "${MQTT_BROKER_HOST:=mosquitto}"
: "${MQTT_BROKER_PORT:=1883}"
: "${WAIT_TIMEOUT:=60}"   # seconds total timeout
: "${SLEEP:=1}"

echo "Waiting for MQTT broker ${MQTT_BROKER_HOST}:${MQTT_BROKER_PORT} (timeout ${WAIT_TIMEOUT}s)..."

end=$((SECONDS + WAIT_TIMEOUT))
while true; do
  # using nc (netcat) — in Debian "nc.traditional" binary is /bin/nc
  if nc -z "${MQTT_BROKER_HOST}" "${MQTT_BROKER_PORT}" 2>/dev/null; then
    echo "Broker is reachable."
    break
  fi

  if [ "${SECONDS}" -ge "${end}" ]; then
    echo "Timed out waiting for broker ${MQTT_BROKER_HOST}:${MQTT_BROKER_PORT}" >&2
    # Still attempt to start publisher (it will log connection errors)
    break
  fi
  sleep "${SLEEP}"
done

# Exec the command from Docker CMD (publisher)
exec "$@"
