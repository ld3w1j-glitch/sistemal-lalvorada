from __future__ import annotations

from .consulta_service import construir_pergunta_contextual


def pergunta_padrao() -> str:
    return construir_pergunta_contextual("lotes")
