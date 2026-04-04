import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text

app = FastAPI()
url = os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1)
engine = create_engine(url, pool_pre_ping=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ntxmesh.com", "https://www.ntxmesh.com", "http://localhost"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/graph")
def get_graph():
    with engine.connect() as conn:
        nodes = conn.execute(text("""
            SELECT node_id, long_name, short_name, hardware,
                   lat, lon, altitude, battery_level,
                   last_seen,
                   EXTRACT(EPOCH FROM (NOW() - last_seen)) AS seconds_since_seen
            FROM nodes ORDER BY last_seen DESC
        """)).mappings().all()

        edges = conn.execute(text("""
            SELECT from_node, to_node, snr, last_updated
            FROM neighbors
            WHERE last_updated > NOW() - INTERVAL '2 hours'
        """)).mappings().all()

    return {
        "nodes": [dict(n) for n in nodes],
        "edges": [dict(e) for e in edges],
    }


@app.get("/api/nodes")
def get_nodes():
    with engine.connect() as conn:
        nodes = conn.execute(text("SELECT * FROM nodes ORDER BY last_seen DESC")).mappings().all()
    return [dict(n) for n in nodes]
