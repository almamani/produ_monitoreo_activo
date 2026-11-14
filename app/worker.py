import asyncio
import logging
import os

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.redis.aio import AsyncRedisSaver
from graph import obtener_informe_validado, workflow
from langgraph.types import Command
from app.observability import configurar_phoenix
from app.jobs import QUEUE, ahora, conectar, guardar, leer



logger = logging.getLogger(__name__)


async def generar_informe(grafo_ventas, pedido, job_id):
    estado = await grafo_ventas.ainvoke(
        {
            "messages": [HumanMessage(content=pedido)],
            "investigacion": None,
            "analisis": None,
            "next_agent": "investigador",
            "instrucciones": "",
            "errores_validacion": [],
            "pasos": 0,
            "informe_final": "",
        },
        config={
            "recursion_limit": 30,
            "configurable": {"thread_id": job_id},
        },
    )
    return estado


async def procesar(redis, grafo_ventas, job_id):
    job = await leer(redis, job_id)
    if job is None or job["status"] != "PENDING":
        return
    job["status"] = "RUNNING"
    if job["started_at"] is None:
        job["started_at"] = ahora()
    await guardar(redis, job)
    try:
        async with asyncio.timeout(180):
            if "decision_revision" in job:
                estado = await grafo_ventas.ainvoke(
                    Command(resume=job["decision_revision"]),
                    config={
                        "recursion_limit": 30,
                        "configurable": {"thread_id": job_id},
                    },
                )
            else:
                estado = await generar_informe(grafo_ventas, job["pedido"], job_id)

        pausas = estado.get("__interrupt__")
        if pausas:
            job.update(status="WAITING_APPROVAL", revision=pausas[0].value)
        elif "decision_revision" in job and estado.get("aprobado") is not True:
            job.update(status="REJECTED", result=None)
        else:
            informe = obtener_informe_validado(estado)
            if not informe:
                raise ValueError("El grafo terminó sin un informe validado")
            job.update(status="DONE", result=informe)
        
    except Exception as exc:
        logger.exception("Error al procesar el trabajo %s", job_id)
        job.update(status="FAILED", error=str(exc))

    if job["status"] != "WAITING_APPROVAL":
        job["finished_at"] = ahora()

    await guardar(redis, job)
    logger.info("%s: %s", job_id, job["status"])


async def main():
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    async with (
        conectar() as redis,
        AsyncRedisSaver.from_conn_string(redis_url) as guardador,
    ):
        await redis.ping()
        await guardador.asetup()
        grafo_ventas = workflow.compile(checkpointer=guardador)
        logger.info("Worker de ventas listo")

        while True:
            item = await redis.blpop(QUEUE, timeout=1)
            if item is not None:
                await procesar(redis, grafo_ventas, item[1])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    proveedor = configurar_phoenix()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker detenido")
    finally:
        proveedor.shutdown()
