import os
import ssl

from paho.mqtt.client import CallbackAPIVersion

from bridger.db import init_db
from bridger.log import logger
from bridger.mqtt import BridgerMQTT

MQTT_BROKER = os.getenv("MQTT_HOST", "localhost")
MQTT_USER = os.getenv("MQTT_USER", "ntxmesh")
MQTT_PASS = os.getenv("MQTT_PASS")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))


if __name__ == "__main__":
    try:
        logger.info("Initializing database...")
        init_db()
        logger.info("Database ready")

        client = BridgerMQTT(CallbackAPIVersion.VERSION2)
        client.username_pw_set(MQTT_USER, MQTT_PASS)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.reconnect_delay_set(min_delay=5, max_delay=120)
        logger.info(f"Connecting to {MQTT_BROKER}:{MQTT_PORT}...")
        client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        client.disconnect()
        client.loop_stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
