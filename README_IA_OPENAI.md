# IA Alvorada com chave OpenAI

Nesta versão, o admin pode ir em **Configurações > IA Alvorada** e colar a chave da API da OpenAI.

O sistema salva a chave na tabela `settings` do banco local e mostra apenas uma versão mascarada.

Rotas adicionadas:

- `POST /configuracoes/openai/testar` — testa a conexão.
- `POST /api/ia/responder` — usada pelo solzinho flutuante da IA.

Observação: a API da OpenAI depende de uma chave válida da conta do usuário.
