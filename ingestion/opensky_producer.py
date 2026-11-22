import json
import time
import logging
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
import os

load_dotenv()

# -----------------------------
# Configuration
# -----------------------------

CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC_AIRCRAFT", "aircraft-positions"
)

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 90))

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)

API_URL = "https://opensky-network.org/api/states/all"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -----------------------------
# OAuth Token Manager
# -----------------------------


class OpenSkyTokenManager:

    def __init__(self):
        self.access_token = None
        self.expires_at = 0

    def get_token(self):

        if time.time() < self.expires_at:
            return self.access_token

        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
            timeout=30,
        )

        response.raise_for_status()

        token_data = response.json()

        self.access_token = token_data["access_token"]

        expires_in = token_data.get("expires_in", 1800)

        # Refresh slightly before actual expiry
        self.expires_at = time.time() + expires_in - 60

        logging.info("OpenSky access token refreshed")

        return self.access_token


# -----------------------------
# Kafka Producer
# -----------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=10,
)

token_manager = OpenSkyTokenManager()


# -----------------------------
# Data Transformation
# -----------------------------


def transform_aircraft(state, snapshot_time):

    return {
        "icao24": state[0],
        "callsign": (state[1] or "").strip(),
        "origin_country": state[2],
        "time_position": state[3],
        "last_contact": state[4],
        "longitude": state[5],
        "latitude": state[6],
        "baro_altitude_m": state[7],
        "on_ground": state[8],
        "velocity_m_s": state[9],
        "heading_deg": state[10],
        "vertical_rate_m_s": state[11],
        "geo_altitude_m": state[13],
        "squawk": state[14],
        "position_source": state[16],
        "aircraft_category": state[17] if len(state) > 17 else None,
        "snapshot_time": snapshot_time,
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }


# -----------------------------
# Fetch & Publish
# -----------------------------


def fetch_aircraft_states():

    token = token_manager.get_token()

    response = requests.get(
        API_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )

    # Token may have expired unexpectedly. Refresh once.
    if response.status_code == 401:

        token_manager.expires_at = 0
        token = token_manager.get_token()

        response = requests.get(
            API_URL,
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )

    response.raise_for_status()

    return response.json()


def publish_aircraft(states_data):

    states = states_data.get("states") or []

    snapshot_time = states_data.get("time")

    published_count = 0

    for state in states:

        # We need aircraft with valid coordinates for our
        # geospatial processing pipeline.
        if state[5] is None or state[6] is None:
            continue

        event = transform_aircraft(state, snapshot_time)

        producer.send(
            KAFKA_TOPIC,
            key=event["icao24"],
            value=event,
        )

        published_count += 1

    producer.flush()

    return published_count


# -----------------------------
# Main Streaming Loop
# -----------------------------


def main():

    logging.info("Starting Aviation Intelligence Producer")
    logging.info("Kafka topic: %s", KAFKA_TOPIC)

    while True:

        cycle_start = time.time()

        try:

            states_data = fetch_aircraft_states()

            count = publish_aircraft(states_data)

            logging.info(
                "Aircraft fetched: %d | Events published: %d",
                len(states_data.get("states") or []),
                count,
            )

        except Exception as error:

            logging.exception("Ingestion cycle failed: %s", error)

        elapsed = time.time() - cycle_start

        sleep_time = max(0, POLL_INTERVAL - elapsed)

        logging.info(
            "Next polling cycle in %.1f seconds",
            sleep_time,
        )

        time.sleep(sleep_time)


if __name__ == "__main__":
    main()