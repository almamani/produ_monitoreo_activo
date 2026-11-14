import os

from openinference.instrumentation.langchain import LangChainInstrumentor
from phoenix.otel import register


def configurar_phoenix():
    proveedor = register(
        project_name=os.getenv("PHOENIX_PROJECT_NAME", "preentrega7-ventas"),
        endpoint=os.getenv(
            "PHOENIX_COLLECTOR_ENDPOINT",
            "http://127.0.0.1:6006/v1/traces",
        ),
        protocol="http/protobuf",
        batch=True,
    )
    LangChainInstrumentor().instrument(tracer_provider=proveedor)
    return proveedor