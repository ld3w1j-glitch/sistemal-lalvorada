"""Camada de inteligência do Sistema Alvorada.

Os módulos daqui organizam as consultas usadas pelo MCP/IA sem misturar
regras de análise com as rotas Flask principais.
"""

from .consulta_service import construir_pergunta_contextual, sugestoes_contextuais

__all__ = ["construir_pergunta_contextual", "sugestoes_contextuais"]
