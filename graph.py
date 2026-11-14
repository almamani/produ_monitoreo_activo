from typing import Literal

from langgraph.graph import END, StateGraph

from agents.analyst_agent import analyst_node
from agents.research_agent import researcher_node
from agents.supervisor import supervisor_node
from agents.synthesis_agent import synthesis_node
from state import AgentState
from langgraph.types import interrupt

import state


def elegir_destino(
    state: AgentState,
) -> Literal["investigador", "analista", "sintesis", "revision_humana", "FINALIZAR"]:
        if state["next_agent"] == "FINALIZAR":
         if destino_al_finalizar(state) == "revision_humana":
            return "revision_humana"
        return state["next_agent"]

def obtener_informe_validado(state: AgentState) -> str:
    informe = state.get("informe_final", "").strip()
    if (
        state.get("errores_validacion")
        or not state.get("investigacion")
        or not state.get("analisis")
        or not informe
        or informe.startswith("Proceso cerrado sin informe.")
    ):
        return ""
    return informe


def destino_al_finalizar(state: AgentState) -> Literal["revision_humana", "terminar"]:
    if obtener_informe_validado(state):
        if any(
            abs(producto.diferencia) >= 20_000
            for producto in state["analisis"].comparaciones
        ):
            return "revision_humana"
    return "terminar"


def revision_humana(state: AgentState) -> dict:
    decision = interrupt({
        "motivo": "Al menos un producto tiene una variación de $20.000 o más.",
        "borrador": state["informe_final"],
    })
    return {"aprobado": decision is True}




workflow = StateGraph(AgentState)

# Registrar los nodos.
workflow.add_node("supervisor", supervisor_node)
workflow.add_node("investigador", researcher_node)
workflow.add_node("analista", analyst_node)
workflow.add_node("sintesis", synthesis_node)
workflow.add_node("revision_humana", revision_humana)

# El supervisor recibe el pedido inicial.
workflow.set_entry_point("supervisor")

# Cada especialista devuelve el control al supervisor.
workflow.add_edge("investigador", "supervisor")
workflow.add_edge("analista", "supervisor")
workflow.add_edge("sintesis", "supervisor")

# La decisión del supervisor determina el próximo destino.
workflow.add_conditional_edges(
    "supervisor",
    elegir_destino,
    {
        "investigador": "investigador",
        "analista": "analista",
        "sintesis": "sintesis",
        "revision_humana": "revision_humana",
        "FINALIZAR": END,
    },
)
workflow.add_edge("revision_humana", END)
app = workflow.compile()