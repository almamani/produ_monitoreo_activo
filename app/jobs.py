import json
import os
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv
from redis.asyncio import Redis


load_dotenv(Path(__file__).resolve().parents[1] / ".env")
QUEUE = "preentrega7:tasks"


def conectar():
    return Redis.from_url(
        os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=5,
    )


def ahora():
    return datetime.now(UTC).isoformat()


def clave(job_id):
    return f"preentrega7:job:{job_id}"


async def leer(redis, job_id):
    datos = await redis.get(clave(job_id))
    return json.loads(datos) if datos is not None else None


async def encolar(redis, job):
    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(clave(job["job_id"]), json.dumps(job, ensure_ascii=False))
        pipe.rpush(QUEUE, job["job_id"])
        await pipe.execute()


async def guardar(redis, job):
    await redis.set(clave(job["job_id"]), json.dumps(job, ensure_ascii=False))
