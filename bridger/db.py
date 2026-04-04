import os

from sqlalchemy import create_engine, text


def _get_engine():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    # SQLAlchemy 2.x requires postgresql:// not postgres://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return create_engine(url)


engine = _get_engine()


def init_db():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                long_name TEXT,
                short_name TEXT,
                hardware TEXT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                altitude DOUBLE PRECISION,
                battery_level INTEGER,
                last_seen TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS neighbors (
                from_node TEXT,
                to_node TEXT,
                snr DOUBLE PRECISION,
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (from_node, to_node)
            )
        """))
        conn.commit()


def upsert_node(node_id, long_name=None, short_name=None, hardware=None,
                lat=None, lon=None, altitude=None, battery_level=None):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO nodes (node_id, long_name, short_name, hardware, lat, lon, altitude, battery_level, last_seen)
            VALUES (:node_id, :long_name, :short_name, :hardware, :lat, :lon, :altitude, :battery_level, NOW())
            ON CONFLICT (node_id) DO UPDATE SET
                long_name = COALESCE(EXCLUDED.long_name, nodes.long_name),
                short_name = COALESCE(EXCLUDED.short_name, nodes.short_name),
                hardware = COALESCE(EXCLUDED.hardware, nodes.hardware),
                lat = COALESCE(EXCLUDED.lat, nodes.lat),
                lon = COALESCE(EXCLUDED.lon, nodes.lon),
                altitude = COALESCE(EXCLUDED.altitude, nodes.altitude),
                battery_level = COALESCE(EXCLUDED.battery_level, nodes.battery_level),
                last_seen = NOW()
        """), {
            "node_id": node_id,
            "long_name": long_name,
            "short_name": short_name,
            "hardware": hardware,
            "lat": lat,
            "lon": lon,
            "altitude": altitude,
            "battery_level": battery_level,
        })
        conn.commit()


def upsert_neighbors(from_node, neighbor_list):
    """neighbor_list: [{"node_id": "!abcd1234", "snr": -5.0}, ...]"""
    with engine.connect() as conn:
        for n in neighbor_list:
            conn.execute(text("""
                INSERT INTO neighbors (from_node, to_node, snr, last_updated)
                VALUES (:from_node, :to_node, :snr, NOW())
                ON CONFLICT (from_node, to_node) DO UPDATE SET
                    snr = EXCLUDED.snr,
                    last_updated = NOW()
            """), {"from_node": from_node, "to_node": n["node_id"], "snr": n.get("snr", 0)})
        conn.commit()
