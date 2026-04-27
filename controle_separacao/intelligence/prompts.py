from __future__ import annotations

CONTEXT_LABELS = {
    "painel": "Painel",
    "estoque": "Estoque",
    "lotes": "Lotes",
    "relatorios": "Relatórios",
    "mcp_teste": "MCP/IA",
}

DEFAULT_CONTEXT_QUESTIONS = {
    "painel": "status do sistema",
    "estoque": "resumo por linha limite 100",
    "lotes": "lotes abertos limite 80",
    "relatorios": "resumo por linha limite 100",
    "mcp_teste": "status do sistema",
}

CONTEXT_SUGGESTIONS = {
    "painel": [
        "status do sistema",
        "estoque baixo até 10 limite 50",
        "lotes abertos limite 50",
        "movimentações do estoque limite 30",
    ],
    "estoque": [
        "resumo por linha limite 100",
        "estoque baixo até 10 limite 80",
        "listar categorias limite 120",
        "buscar produtos com saldo entre 1 e 20 limite 80",
        "movimentações do estoque limite 50",
    ],
    "lotes": [
        "lotes abertos limite 80",
        "listar lojas limite 120",
        "status do sistema",
        "movimentações do estoque limite 50",
    ],
    "relatorios": [
        "resumo por linha limite 100",
        "estoque baixo até 10 limite 80",
        "lotes abertos limite 80",
        "status do sistema",
    ],
    "mcp_teste": [
        "status do sistema",
        "resumo por linha limite 100",
        "listar categorias limite 120",
        "estoque baixo até 10 limite 80",
    ],
}

CONTEXT_HELP_TEXT = {
    "painel": "Resumo geral do sistema, estoque, lotes e movimentações.",
    "estoque": "Use para localizar itens, categorias, saldo baixo e movimentações.",
    "lotes": "Use para consultar lotes abertos, lojas e pontos de atenção.",
    "relatorios": "Use para montar resumos gerenciais e exportar análises.",
    "mcp_teste": "Central livre de consulta MCP/IA.",
}
