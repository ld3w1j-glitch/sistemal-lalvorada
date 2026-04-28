# MCP inteligente - melhoria aplicada

Esta versão adiciona uma camada simples e segura de inteligência ao MCP.

## O que foi adicionado

- Calculadora segura dentro do chat MCP.
- Entendimento de comandos como:
  - `calcule 10*3`
  - `quanto é 10 vezes 3`
  - `10 + 5 * 2`
  - `(10 + 5) / 3`
- O cálculo não usa `eval` direto.
- O sistema usa `ast` com lista branca de operações permitidas.
- Em ações sensíveis, permanece a exigência de senha já existente.

## Segurança

A calculadora aceita apenas:

- números
- `+`
- `-`
- `*`
- `/`
- `%`
- `//`
- `**`
- parênteses

Ela não executa código Python livre.

## Onde foi alterado

Arquivo principal:

- `controle_separacao/core.py`

Funções adicionadas:

- `_mcp_avaliar_no_calculo`
- `_mcp_normalizar_expressao_calculo`
- `_mcp_tentar_calculo`

O roteador `_executar_pergunta_mcp` agora tenta identificar cálculo antes de chamar as ferramentas de estoque/lotes.
