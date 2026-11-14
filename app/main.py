import json

from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, StringConstraints, StrictBool
from redis.exceptions import RedisError, WatchError

from app.jobs import ahora, conectar, encolar, leer, QUEUE, clave


class TaskRequest(BaseModel):
    pedido: Annotated[str, StringConstraints(
        strip_whitespace=True, min_length=1, max_length=4000
    )]


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with conectar() as redis:
        await redis.ping()
        app.state.redis = redis
        yield


app = FastAPI(title="Preentrega 7 · Orquestador de ventas", lifespan=lifespan)


@app.post("/tasks", status_code=202)
async def crear_tarea(payload: TaskRequest, request: Request):
    job_id = str(uuid4())
    job = {
        "job_id": job_id,
        "pedido": payload.pedido,
        "status": "PENDING",
        "result": None,
        "error": None,
        "created_at": ahora(),
        "started_at": None,
        "finished_at": None,
    }
    try:
        await encolar(request.app.state.redis, job)
    except RedisError as error:
        raise HTTPException(503, "No se pudo confirmar el encolado en Redis") from error
    return {"job_id": job_id, "status": "PENDING", "status_url": f"/tasks/{job_id}"}


@app.get("/tasks/{job_id}")
async def consultar_tarea(job_id: UUID, request: Request):
    try:
        job = await leer(request.app.state.redis, str(job_id))
    except RedisError as error:
        raise HTTPException(503, "Redis no está disponible") from error
    if job is None:
        raise HTTPException(404, "Trabajo no encontrado")
    return job

class RevisionRequest(BaseModel):
    aprobado: StrictBool


@app.post("/tasks/{job_id}/approve", status_code=202)
async def decidir_revision(job_id: UUID, payload: RevisionRequest, request: Request):
    redis = request.app.state.redis
    id_tarea = str(job_id)

    try:
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(clave(id_tarea))
            job = await leer(pipe, id_tarea)

            if job is None:
                raise HTTPException(404, "Trabajo no encontrado")
            if job["status"] != "WAITING_APPROVAL":
                raise HTTPException(409, "La tarea no está esperando una decisión")

            job.update(status="PENDING", decision_revision=payload.aprobado)
            pipe.multi()
            pipe.set(clave(id_tarea), json.dumps(job, ensure_ascii=False))
            pipe.rpush(QUEUE, id_tarea)
            await pipe.execute()
    except WatchError as error:
        raise HTTPException(409, "La tarea cambió mientras se enviaba la decisión") from error
    except RedisError as error:
        raise HTTPException(503, "No se pudo confirmar la decisión en Redis") from error

    return {
        "job_id": id_tarea,
        "status": "PENDING",
        "status_url": f"/tasks/{id_tarea}",
    }