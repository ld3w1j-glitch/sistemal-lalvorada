from __future__ import annotations

from .consulta_service import construir_pergunta_contextual


def pergunta_padrao(linha: str = "") -> str:
    return construir_pergunta_contextual("relatorios", "", linha)
