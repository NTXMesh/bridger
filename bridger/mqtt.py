import base64
import os
from collections import deque

from google.protobuf.message import DecodeError
from meshtastic.protobuf.mqtt_pb2 import ServiceEnvelope
from paho.mqtt.client import Client

from bridger.dataclasses import DeviceTelemetryPoint, NeighborInfoPacket, NodeInfoPoint, PositionPoint
from bridger.db import upsert_neighbors, upsert_node
from bridger.log import logger
from bridger.mesh import PacketProcessorError, PBPacketProcessor

MQTT_TOPIC = os.getenv("MQTT_TOPIC", "msh/#")


def _node_hex(raw_id: int) -> str:
    return f"!{raw_id:08x}"


class BridgerMQTT(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message_queue = deque(maxlen=100)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logger.error(f"Connection failed with code: {reason_code}. Attempting to reconnect...")
            return

        subscription = self.subscribe(MQTT_TOPIC)
        if subscription[0] != 0:
            logger.error(f"Failed to subscribe to topic: {MQTT_TOPIC}")
            return

        logger.info(f"Connected and subscribed to topic: {MQTT_TOPIC}")

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Disconnected")

    def on_message(self, client, userdata, message):
        message_payload = base64.b64encode(message.payload)
        breadcrumb_data = {"topic": message.topic, "payload": message_payload}

        logger.bind(**breadcrumb_data).opt(colors=True).debug(
            f"MQTT message on topic <green>{message.topic}</green>: {message_payload}"
        )

        try:
            service_envelope = ServiceEnvelope.FromString(message.payload)
            packet_id = service_envelope.packet.id
            gateway_id = service_envelope.gateway_id

            if packet_id in self.message_queue:
                logger.bind(envelope_id=packet_id).opt(colors=True).debug(
                    f"Packet <yellow>{packet_id}</yellow> from <green>{gateway_id}</green> already in queue"
                )
                return

            self.message_queue.append(packet_id)

            pb_processor = PBPacketProcessor(service_envelope)
            data = pb_processor.data

            if data is None:
                logger.bind(envelope_id=packet_id).debug("No data to write")
                return

            from_id = _node_hex(getattr(service_envelope.packet, "from"))

            if isinstance(data, NodeInfoPoint):
                upsert_node(from_id,
                            long_name=data.long_name,
                            short_name=data.short_name,
                            hardware=str(data.hw_model) if data.hw_model else None)
                logger.info(f"Updated node info for {from_id} ({data.long_name})")

            elif isinstance(data, PositionPoint):
                upsert_node(from_id,
                            lat=data.latitude_i / 1e7,
                            lon=data.longitude_i / 1e7,
                            altitude=data.altitude if data.altitude else None)
                logger.info(f"Updated position for {from_id}")

            elif isinstance(data, DeviceTelemetryPoint):
                if data.battery_level is not None:
                    upsert_node(from_id, battery_level=data.battery_level)
                    logger.info(f"Updated battery for {from_id}: {data.battery_level}%")

            elif isinstance(data, list) and data and isinstance(data[0], NeighborInfoPacket):
                upsert_node(from_id)
                neighbors = [{"node_id": _node_hex(n.neighbor_id), "snr": n.snr} for n in data]
                upsert_neighbors(from_id, neighbors)
                logger.info(f"Updated {len(neighbors)} neighbors for {from_id}")

            else:
                logger.bind(envelope_id=packet_id).debug(f"Skipping unhandled data type: {type(data)}")

        except DecodeError as e:
            self._handle_decode_error(e, breadcrumb_data, message.payload)
        except (TypeError, AttributeError) as e:
            logger.bind(**breadcrumb_data).exception(f"Error: {e}")
        except PacketProcessorError as e:
            logger.info(e)

    def _handle_decode_error(self, error, breadcrumb_data, payload):
        logger.bind(**breadcrumb_data).warning(f"Cannot decode protobuf: {error}")

        try:
            json_payload = payload.decode("utf-8")
            logger.bind(**breadcrumb_data).debug(f"Message payload: \n{json_payload}")
        except UnicodeDecodeError:
            logger.bind(**breadcrumb_data).warning("Message payload is not JSON either")
