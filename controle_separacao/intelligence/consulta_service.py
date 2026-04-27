from __future__ import annotations

from typing import Any

from .prompts import CONTEXT_HELP_TEXT, CONTEXT_LABELS, CONTEXT_SUGGESTIONS, DEFAULT_CONTEXT_QUESTIONS


def normalizar_contexto(contexto: Any) -> str:
    valor = str(contexto or "").strip().lower()
    return valor if valor in CONTEXT_LABELS else "mcp_teste"


def construir_pergunta_contextual(contexto: Any, pergunta: str = "", linha: str = "") -> str:
    """Define uma pergunta útil quando o usuário clica em analisar a página.

    Quando o usuário já digitou algo, preservamos a intenção dele. Quando apenas
    clica em "Analisar esta página", usamos uma consulta padrão apropriada para
    a tela atual.
    """
    ctx = normalizar_contexto(contexto)
    texto = str(pergunta or "").strip()
    if texto:
        return texto

    base = DEFAULT_CONTEXT_QUESTIONS.get(ctx, DEFAULT_CONTEXT_QUESTIONS["mcp_teste"])
    linha_limpa = str(linha or "").strip()
    if linha_limpa and ctx in {"estoque", "relatorios", "mcp_teste"} and "linha" not in base.casefold():
        return f"{base} linha {linha_limpa}"
    return base


def sugestoes_contextuais(contexto: Any) -> dict[str, Any]:
    ctx = normalizar_contexto(contexto)
    return {
        "contexto": ctx,
        "label": CONTEXT_LABELS.get(ctx, "MCP/IA"),
        "help": CONTEXT_HELP_TEXT.get(ctx, "Consulta inteligente do sistema."),
        "sugestoes": list(CONTEXT_SUGGESTIONS.get(ctx, CONTEXT_SUGGESTIONS["mcp_teste"])),
    }
