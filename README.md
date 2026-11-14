# Preentrega 7 · API asíncrona para el orquestador de ventas

FastAPI recibe pedidos de comparación de ventas mensuales y los encola en Redis. Un worker separado ejecuta el grafo de la Preentrega 6 y guarda el estado de la tarea. El grafo coordina un supervisor, un investigador, un analista y un nodo de síntesis. Usa seis registros ficticios de SQLite y el modelo `gpt-4o-mini` mediante la API de OpenAI.

Redis conserva también los checkpoints de LangGraph. Cuando la diferencia absoluta en el importe de algún producto alcanza **ARS 20.000**, el grafo pausa la entrega del informe para revisión humana. El worker usa el `job_id` como `thread_id` para retomar el checkpoint. Phoenix recibe las trazas de las ejecuciones del worker.

## Preparación inicial

Con Python 3.12 instalado, abrí PowerShell en la raíz del repositorio, donde están `requirements.txt`, `setup_db.py` y la carpeta `app/`. Creá el entorno si es la primera vez.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
.\.venv\Scripts\python.exe setup_db.py
```

Completá `OPENAI_API_KEY` en `.env` y mantené ese archivo fuera del repositorio. El comando de SQLite debe mostrar `Registros disponibles: 6`. El worker toma `REDIS_URL` de `.env` y, si falta, usa `redis://127.0.0.1:6379/0`. Phoenix se configura mediante `PHOENIX_COLLECTOR_ENDPOINT` y `PHOENIX_PROJECT_NAME`, con valores predeterminados `http://127.0.0.1:6006/v1/traces` y `preentrega7-ventas`.

## Arranque local

### 1. Redis en Ubuntu (WSL)

Esta práctica usa el Redis instalado mediante Snap. Después de abrir Ubuntu, verificá si el servicio está activo y qué módulos tiene la instancia del puerto 6379.

```bash
snap services redis
snap run redis.cli ping
snap run redis.cli INFO modules
```

Buscá `PONG`, `ReJSON` y `search`. Si el servicio de Snap está inactivo, inicialo con `sudo snap start redis.server`. Si otra instancia ocupa el puerto, verificá con `sudo ss -ltnp 'sport = :6379'`. En el entorno de esta prueba, cuando el servicio de Ubuntu `redis-server.service` ocupaba ese puerto, se lo detenía con `sudo systemctl stop redis-server.service` y luego se iniciaba el de Snap. Volvé a verificar el ping y los módulos.

### 2. Phoenix en su propia terminal de PowerShell

Usá el entorno donde está instalado el servidor de Phoenix (`arize-phoenix`). Con ese entorno activado, ejecutá:

```powershell
python -m phoenix.server.main serve
```

Dejá la terminal abierta y comprobá que abre el [panel de Phoenix](http://127.0.0.1:6006).

### 3. API en una terminal de PowerShell

Desde la raíz del repositorio:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --port 8002
```

Cuando aparezca `Application startup complete`, abrí [Swagger](http://127.0.0.1:8002/docs).

### 4. Worker en otra terminal de PowerShell

Desde la misma carpeta:

```powershell
.\.venv\Scripts\python.exe -m app.worker
```

`Worker de ventas listo` indica que el proceso se conectó a Redis e inicializó el guardador de checkpoints. Dejá esta terminal abierta.

## Probar el flujo desde Swagger

1. Enviá a `POST /tasks` este cuerpo. La API devuelve HTTP 202 con `job_id` y `status_url`.

   ```json
   {
     "pedido": "Compará las ventas de septiembre de 2026 como período base contra octubre de 2026 como período actual. Calculá la variación por producto y prepará un informe con conclusiones y fuentes."
   }
   ```

2. Consultá `GET /tasks/{job_id}`. El trabajo se encola como `PENDING` y pasa a `RUNNING` cuando el worker lo toma. Este pedido activa la regla de revisión. En `WAITING_APPROVAL`, el borrador está en `revision.borrador` y `result` sigue en `null`.
3. Para comprobar la persistencia, podés reiniciar el worker mientras el trabajo está en pausa, con Redis activo. Enviá a `POST /tasks/{job_id}/approve` el cuerpo `{"aprobado": true}` para aprobar o `{"aprobado": false}` para rechazar. La API devuelve HTTP 202 y encola la decisión.
4. Consultá el mismo `job_id`. Una aprobación termina en `DONE` con el informe y un rechazo en `REJECTED` con `result: null`. Si falla el procesamiento, el estado es `FAILED`. Para probar las dos decisiones usá dos tareas distintas.

## Prueba de cinco peticiones concurrentes

Dejá Redis, la API y Phoenix activos. Reiniciá el worker con un nombre de proyecto de Phoenix reservado para esta corrida. Usá un nombre nuevo en cada repetición para medir solo las trazas de ese lote.

```powershell
$env:PHOENIX_PROJECT_NAME = "preentrega7-carga-5"
.\.venv\Scripts\python.exe -m app.worker
```

En otra terminal, desde la raíz del repositorio, ejecutá este bloque. Imprime los cinco IDs y consulta sus estados hasta observar la pausa o un estado final.

```powershell
@'
import asyncio
import httpx

PEDIDO = (
    "Compara las ventas de septiembre de 2026 como periodo base contra "
    "octubre de 2026 como periodo actual. Calcula la variacion por producto "
    "y prepara un informe con conclusiones y fuentes."
)
ESTADOS_DE_SALIDA = {"WAITING_APPROVAL", "DONE", "FAILED", "REJECTED"}


async def enviar(cliente, numero):
    respuesta = await cliente.post("/tasks", json={"pedido": PEDIDO})
    respuesta.raise_for_status()
    tarea = respuesta.json()
    print(f"POST {numero}: {respuesta.status_code} | {tarea['job_id']}", flush=True)
    return tarea["status_url"]


async def main():
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8002", timeout=10) as cliente:
        pendientes = set(await asyncio.gather(
            *(enviar(cliente, numero) for numero in range(1, 6))
        ))

        async with asyncio.timeout(960):
            while pendientes:
                for ruta in sorted(pendientes):
                    respuesta = await cliente.get(ruta)
                    respuesta.raise_for_status()
                    tarea = respuesta.json()
                    if tarea["status"] in ESTADOS_DE_SALIDA:
                        print(f"GET {tarea['job_id']}: {tarea['status']}", flush=True)
                        if tarea.get("error"):
                            print(tarea["error"], flush=True)
                        pendientes.remove(ruta)
                if pendientes:
                    await asyncio.sleep(2)


asyncio.run(main())
'@ | .\.venv\Scripts\python.exe -
```

La API recibió las cinco peticiones de forma concurrente y un solo worker procesó las tareas en secuencia. Las cinco llegaron a `WAITING_APPROVAL`. Phoenix mide la ejecución del grafo hasta la pausa. El tiempo en cola y la espera por la decisión humana quedan fuera de esta medición.

| Métrica | Resultado | Evidencia |
| --- | --- | --- |
| Costo acumulado por ejecución | Inferior a USD 0,01 en cada una de las cinco trazas | [Costo por ejecución](screenshots/costo_por_ejecucion.png) |
| Latencia p95 del grafo | 19,57 s en **Trace Latency** | [Latencia p95](screenshots/latencia_p95.png) |
| Detalle de nodos | En la traza de 17,2 s, el supervisor acumuló 5.428 de 6.672 tokens en cuatro intervenciones y la síntesis tuvo la mayor duración, con 9,8 s | [Traza por nodo](screenshots/traza_por_nodo.png) |

## Evidencias de la API

| Respuesta de `GET /tasks/{job_id}` | Qué verifica |
| --- | --- |
| [Ejecución asíncrona](evidencias/01_ejecucion_asincrona.json) | La primera tarea llegó a `DONE` y la API devolvió el informe con fecha de cierre. |
| [Espera de aprobación](evidencias/02_esperando_aprobacion.json) | El grafo pausó y el borrador quedó disponible para la revisión. |
| [Rechazo](evidencias/03_rechazo.json) | La tarea pausada pasó a `REJECTED` con el mismo ID después de reiniciar el worker y registrar la decisión. |
| [Aprobación](evidencias/04_aprobacion.json) | La tarea aprobada llegó a `DONE` con el informe, la decisión y la fecha de cierre. |

La evidencia de rechazo es anterior al ajuste que guarda `finished_at` en los estados finales. La prueba posterior de aprobación verificó la fecha de cierre. En esta implementación, la reanudación desde un checkpoint se probó durante la espera de aprobación. Un cierre abrupto del worker después de retirar un trabajo de la cola puede dejarlo pendiente de recuperación manual.
