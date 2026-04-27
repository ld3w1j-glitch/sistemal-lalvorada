from __future__ import annotations

import json
import sqlite3
import unicodedata
from contextlib import closing
from typing import Any

from mcp.server.fastmcp import FastMCP

from .core import DB_PATH, ensure_default_data, fmt_money, fmt_num, get_conn


mcp = FastMCP(
    "Sistema Alvorada",
    instructions=(
        "Servidor MCP de leitura do Sistema Alvorada. "
        "Use as ferramentas para consultar estoque, lojas, lotes, separações e movimentações. "
        "Esta versão não altera dados do sistema."
    ),
)

MAX_LIMIT = 200


def _limit(value: int | None, default: int = 50) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, MAX_LIMIT))


def _to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [_row_to_dict(row) or {} for row in rows]


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def _status_sistema_data() -> dict[str, Any]:
    ensure_default_data()
    with closing(get_conn()) as conn:
        produtos_ativos = conn.execute("SELECT COUNT(*) AS total FROM stock_items WHERE ativo = 1").fetchone()["total"]
        produtos_sem_saldo = conn.execute(
            "SELECT COUNT(*) AS total FROM stock_items WHERE ativo = 1 AND quantidade_atual <= 0"
        ).fetchone()["total"]
        produtos_estoque_baixo = conn.execute(
            "SELECT COUNT(*) AS total FROM stock_items WHERE ativo = 1 AND quantidade_atual > 0 AND quantidade_atual <= 10"
        ).fetchone()["total"]
        lojas_ativas = conn.execute("SELECT COUNT(*) AS total FROM stores WHERE ativo = 1").fetchone()["total"]
        lotes_abertos = conn.execute(
            "SELECT COUNT(DISTINCT lote_codigo) AS total FROM separations WHERE status <> 'FINALIZADA'"
        ).fetchone()["total"]
        separacoes_abertas = conn.execute(
            "SELECT COUNT(*) AS total FROM separations WHERE status <> 'FINALIZADA'"
        ).fetchone()["total"]
        movimentacoes = conn.execute("SELECT COUNT(*) AS total FROM stock_movements").fetchone()["total"]

    return {
        "status": "ok",
        "db_path": DB_PATH,
        "produtos_ativos": produtos_ativos,
        "produtos_sem_saldo": produtos_sem_saldo,
        "produtos_estoque_baixo_ate_10": produtos_estoque_baixo,
        "lojas_ativas": lojas_ativas,
        "lotes_abertos": lotes_abertos,
        "separacoes_abertas": separacoes_abertas,
        "movimentacoes_estoque": movimentacoes,
        "observacao": "MCP em modo leitura. Nenhuma ferramenta desta versão altera o banco de dados.",
    }


@mcp.tool()
def status_sistema() -> dict[str, Any]:
    """Mostra um resumo geral do Sistema Alvorada e confirma qual banco SQLite está sendo usado."""
    return _status_sistema_data()



def _normalizar_linha_texto(valor: Any) -> str:
    texto = unicodedata.normalize("NFKD", str(valor or ""))
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.upper()
    texto = "".join(ch if ch.isalnum() else " " for ch in texto)
    return " ".join(texto.split())


_STOPWORDS_LINHA = {
    "DE", "DA", "DO", "DAS", "DOS", "E", "A", "O", "AS", "OS", "COM", "SEM", "PARA",
    "KG", "G", "GR", "GRS", "GRAMAS", "ML", "L", "LT", "UN", "UND", "UNID", "UNIDADE",
    "CX", "C", "PC", "PCT", "PACOTE", "BDJ", "BAND", "BANDJA", "BANDJ", "BAG", "SACO",
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
}

_LINHAS_PRIORITARIAS = [
    ("PÃO DE ALHO", {"PAO", "ALHO"}),
    ("LINGUIÇA", {"LINGUICA"}),
    ("SALSICHA", {"SALSICHA"}),
    ("FRANGO", {"FRANGO"}),
    ("CARNES", {"CARNE"}),
    ("COSTELA", {"COSTELA"}),
    ("BACON", {"BACON"}),
    ("PRESUNTO", {"PRESUNTO"}),
    ("MORTADELA", {"MORTADELA"}),
    ("SALAME", {"SALAME"}),
    ("QUEIJOS", {"QUEIJO"}),
    ("REQUEIJÃO", {"REQUEIJAO"}),
    ("MANTEIGA", {"MANTEIGA"}),
    ("LEITE", {"LEITE"}),
    ("IOGURTES", {"IOGURTE"}),
    ("MASSAS", {"MASSA"}),
    ("LASANHAS", {"LASANHA"}),
    ("PIZZAS", {"PIZZA"}),
    ("HAMBÚRGUER", {"HAMBURGUER"}),
    ("BATATA", {"BATATA"}),
    ("POLPA", {"POLPA"}),
    ("SORVETES", {"SORVETE"}),
    ("BEBIDAS", {"REFRIGERANTE"}),
    ("ÁGUA", {"AGUA"}),
    ("SUCO", {"SUCO"}),
    ("FARINHAS", {"FARINHA"}),
    ("AÇÚCAR", {"ACUCAR"}),
    ("ARROZ", {"ARROZ"}),
    ("FEIJÃO", {"FEIJAO"}),
    ("CAFÉ", {"CAFE"}),
    ("ÓLEOS", {"OLEO"}),
    ("MOLHOS", {"MOLHO"}),
    ("TEMPEROS", {"TEMPERO"}),
    ("DESCARTÁVEIS", {"DESCARTAVEL"}),
]


def _linha_do_produto(descricao: Any, linha_erp: Any = "") -> str:
    """Cria uma linha/categoria automática a partir da descrição do item, priorizando a linha importada do ERP."""
    linha_erp_txt = str(linha_erp or "").strip()
    if linha_erp_txt:
        return linha_erp_txt
    normalizado = _normalizar_linha_texto(descricao)
    tokens = normalizado.split()
    token_set = set(tokens)

    for nome, obrigatorios in _LINHAS_PRIORITARIAS:
        if obrigatorios.issubset(token_set):
            return nome

    uteis: list[str] = []
    for token in tokens:
        if token in _STOPWORDS_LINHA:
            continue
        if token.isdigit():
            continue
        if any(ch.isdigit() for ch in token) and len(token) <= 6:
            continue
        uteis.append(token)
        if len(uteis) >= 2:
            break

    if not uteis:
        return "OUTROS"

    return " ".join(uteis).title()


def _linha_confere(descricao: Any, linha: str = "", linha_erp: Any = "") -> bool:
    linha_limpa = str(linha or "").strip()
    if not linha_limpa:
        return True

    linha_item = _normalizar_linha_texto(_linha_do_produto(descricao, linha_erp))
    filtro = _normalizar_linha_texto(linha_limpa)
    if linha_item == filtro:
        return True

    descricao_norm = _normalizar_linha_texto(descricao)
    partes = [p for p in filtro.split() if p not in _STOPWORDS_LINHA]
    return bool(partes) and all(parte in descricao_norm.split() for parte in partes)


def _row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def _produto_dict(row: sqlite3.Row) -> dict[str, Any]:
    quantidade = _to_float(row["quantidade_atual"])
    custo = _to_float(row["custo_unitario"])
    fator = _to_float(row["fator_embalagem"]) or 1.0
    return {
        "id": row["id"],
        "linha": _linha_do_produto(row["descricao"], _row_get(row, "linha_erp", "")),
        "linha_erp": _row_get(row, "linha_erp", ""),
        "erp_loja": _row_get(row, "erp_loja", ""),
        "erp_data_base": _row_get(row, "erp_data_base", ""),
        "codigo": row["codigo"],
        "codigo_barras": row["codigo_barras"],
        "descricao": row["descricao"],
        "fator_embalagem": fator,
        "fator_formatado": f"Emb{fmt_num(fator)}",
        "quantidade_atual": quantidade,
        "quantidade_formatada": fmt_num(quantidade),
        "quantidade_em_embalagens": quantidade / fator if fator > 0 else quantidade,
        "custo_unitario": custo,
        "custo_unitario_formatado": fmt_money(custo),
        "valor_total_estimado": quantidade * custo,
        "valor_total_estimado_formatado": fmt_money(quantidade * custo),
        "atualizado_em": row["atualizado_em"],
    }


@mcp.tool()
def listar_categorias_linha(termo: str = "", limite: int = 80) -> list[dict[str, Any]]:
    """Lista categorias/linhas automáticas encontradas nas descrições dos produtos."""
    termo_limpo = str(termo or "").strip()
    limite_seguro = _limit(limite, default=80)
    where = ["ativo = 1"]
    params: list[Any] = []

    if termo_limpo:
        like = f"%{termo_limpo}%"
        where.append("(codigo LIKE ? OR codigo_barras LIKE ? OR descricao LIKE ?)")
        params.extend([like, like, like])

    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(
            f"""
            SELECT descricao, linha_erp
            FROM stock_items
            WHERE {' AND '.join(where)}
            ORDER BY descricao COLLATE NOCASE ASC
            LIMIT 5000
            """,
            params,
        ).fetchall()

    grupos: dict[str, dict[str, Any]] = {}
    for row in rows:
        linha = _linha_do_produto(row["descricao"], _row_get(row, "linha_erp", ""))
        if linha not in grupos:
            grupos[linha] = {"linha": linha, "total_itens": 0, "amostra": row["descricao"]}
        grupos[linha]["total_itens"] += 1

    categorias = sorted(grupos.values(), key=lambda item: (-int(item["total_itens"]), item["linha"]))
    return categorias[:limite_seguro]


@mcp.tool()
def consultar_produto(termo: str) -> dict[str, Any]:
    """Consulta um produto ativo pelo código interno ou código de barras."""
    termo_limpo = str(termo or "").strip()
    if not termo_limpo:
        return {"encontrado": False, "mensagem": "Informe um código ou código de barras."}

    ensure_default_data()
    with closing(get_conn()) as conn:
        item = conn.execute(
            """
            SELECT id, codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, linha_erp, erp_loja, erp_data_base, atualizado_em
            FROM stock_items
            WHERE ativo = 1
              AND (codigo = ? OR codigo_barras = ?)
            LIMIT 1
            """,
            (termo_limpo, termo_limpo),
        ).fetchone()

    if item is None:
        return {"encontrado": False, "termo": termo_limpo, "mensagem": "Produto não encontrado."}

    produto = _produto_dict(item)
    produto["encontrado"] = True
    return produto


@mcp.tool()
def listar_produtos_estoque(
    termo: str = "",
    somente_com_saldo: bool = False,
    limite: int = 50,
    linha: str = "",
) -> list[dict[str, Any]]:
    """Lista produtos ativos do estoque, com filtro opcional por termo e por linha/categoria."""
    termo_limpo = str(termo or "").strip()
    linha_limpa = str(linha or "").strip()
    limite_seguro = _limit(limite)
    where = ["ativo = 1"]
    params: list[Any] = []

    if termo_limpo:
        like = f"%{termo_limpo}%"
        where.append("(codigo LIKE ? OR codigo_barras LIKE ? OR descricao LIKE ?)")
        params.extend([like, like, like])

    if somente_com_saldo:
        where.append("quantidade_atual > 0")

    # Quando existe filtro por linha, buscamos uma margem maior e aplicamos a linha automática em Python.
    sql_limit = 5000 if linha_limpa else limite_seguro
    sql = f"""
        SELECT id, codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, linha_erp, erp_loja, erp_data_base, atualizado_em
        FROM stock_items
        WHERE {' AND '.join(where)}
        ORDER BY descricao COLLATE NOCASE ASC
        LIMIT ?
    """
    params.append(sql_limit)

    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(sql, params).fetchall()

    resultado: list[dict[str, Any]] = []
    for row in rows:
        if not _linha_confere(row["descricao"], linha_limpa, _row_get(row, "linha_erp", "")):
            continue
        resultado.append(_produto_dict(row))
        if len(resultado) >= limite_seguro:
            break
    return resultado


@mcp.tool()
def listar_estoque_baixo(
    limite_quantidade: float = 10,
    limite: int = 50,
    linha: str = "",
) -> list[dict[str, Any]]:
    """Lista produtos ativos com quantidade menor ou igual ao limite informado, podendo filtrar por linha/categoria."""
    limite_resultados = _limit(limite)
    limite_qtd = _to_float(limite_quantidade)
    linha_limpa = str(linha or "").strip()
    if limite_qtd < 0:
        limite_qtd = 0

    sql_limit = 5000 if linha_limpa else limite_resultados
    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id, codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, linha_erp, erp_loja, erp_data_base, atualizado_em
            FROM stock_items
            WHERE ativo = 1 AND quantidade_atual <= ?
            ORDER BY quantidade_atual ASC, descricao COLLATE NOCASE ASC
            LIMIT ?
            """,
            (limite_qtd, sql_limit),
        ).fetchall()

    resultado = []
    for row in rows:
        if not _linha_confere(row["descricao"], linha_limpa, _row_get(row, "linha_erp", "")):
            continue
        resultado.append(_produto_dict(row))
        if len(resultado) >= limite_resultados:
            break
    return resultado


def listar_produtos_estoque_resultado(rows: list[sqlite3.Row], linha: str = "") -> list[dict[str, Any]]:
    resultado: list[dict[str, Any]] = []
    linha_limpa = str(linha or "").strip()
    for row in rows:
        if not _linha_confere(row["descricao"], linha_limpa, _row_get(row, "linha_erp", "")):
            continue
        resultado.append(_produto_dict(row))
    return resultado


@mcp.tool()
def buscar_produtos_avancado(
    termo: str = "",
    linha: str = "",
    estoque_min: float | None = None,
    estoque_max: float | None = None,
    somente_com_saldo: bool = False,
    ordenar: str = "descricao",
    direcao: str = "asc",
    limite: int = 50,
) -> list[dict[str, Any]]:
    """Busca produtos com filtros avançados: termo, linha/categoria, saldo mínimo, saldo máximo e ordenação."""
    termo_limpo = str(termo or "").strip()
    linha_limpa = str(linha or "").strip()
    limite_seguro = _limit(limite)
    where = ["ativo = 1"]
    params: list[Any] = []

    if termo_limpo:
        like = f"%{termo_limpo}%"
        where.append("(codigo LIKE ? OR codigo_barras LIKE ? OR descricao LIKE ?)")
        params.extend([like, like, like])

    if somente_com_saldo:
        where.append("quantidade_atual > 0")

    if estoque_min is not None:
        where.append("quantidade_atual >= ?")
        params.append(_to_float(estoque_min))

    if estoque_max is not None:
        where.append("quantidade_atual <= ?")
        params.append(_to_float(estoque_max))

    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(
            f"""
            SELECT id, codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, linha_erp, erp_loja, erp_data_base, atualizado_em
            FROM stock_items
            WHERE {' AND '.join(where)}
            ORDER BY descricao COLLATE NOCASE ASC
            LIMIT 5000
            """,
            params,
        ).fetchall()

    produtos: list[dict[str, Any]] = []
    for row in rows:
        if not _linha_confere(row["descricao"], linha_limpa, _row_get(row, "linha_erp", "")):
            continue
        produtos.append(_produto_dict(row))

    ordenar_norm = _normalizar_linha_texto(ordenar or "descricao")
    reverse = str(direcao or "asc").strip().lower() in {"desc", "decrescente", "maior"}

    def chave(produto: dict[str, Any]) -> Any:
        if ordenar_norm in {"CODIGO", "COD"}:
            return str(produto.get("codigo") or "")
        if ordenar_norm in {"QUANTIDADE", "SALDO", "ESTOQUE", "QTD"}:
            return _to_float(produto.get("quantidade_atual"))
        if ordenar_norm in {"VALOR", "CUSTO", "TOTAL"}:
            return _to_float(produto.get("valor_total_estimado"))
        if ordenar_norm in {"LINHA", "CATEGORIA"}:
            return str(produto.get("linha") or "")
        return str(produto.get("descricao") or "")

    produtos.sort(key=chave, reverse=reverse)
    return produtos[:limite_seguro]


@mcp.tool()
def resumo_estoque_por_linha(linha: str = "", limite: int = 100) -> list[dict[str, Any]]:
    """Agrupa o estoque por linha/categoria e mostra quantidade, valor estimado, itens zerados e estoque baixo."""
    linha_limpa = str(linha or "").strip()
    limite_seguro = _limit(limite, default=100)

    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id, codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, linha_erp, erp_loja, erp_data_base, atualizado_em
            FROM stock_items
            WHERE ativo = 1
            ORDER BY descricao COLLATE NOCASE ASC
            LIMIT 10000
            """
        ).fetchall()

    grupos: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not _linha_confere(row["descricao"], linha_limpa, _row_get(row, "linha_erp", "")):
            continue
        produto = _produto_dict(row)
        grupo = grupos.setdefault(
            produto["linha"],
            {
                "linha": produto["linha"],
                "total_itens": 0,
                "quantidade_total": 0.0,
                "valor_total_estimado": 0.0,
                "itens_zerados": 0,
                "itens_estoque_baixo_ate_10": 0,
                "menor_estoque": None,
                "maior_estoque": None,
                "amostra": produto.get("descricao") or "",
            },
        )
        quantidade = _to_float(produto.get("quantidade_atual"))
        valor = _to_float(produto.get("valor_total_estimado"))
        grupo["total_itens"] += 1
        grupo["quantidade_total"] += quantidade
        grupo["valor_total_estimado"] += valor
        if quantidade <= 0:
            grupo["itens_zerados"] += 1
        if quantidade <= 10:
            grupo["itens_estoque_baixo_ate_10"] += 1
        grupo["menor_estoque"] = quantidade if grupo["menor_estoque"] is None else min(_to_float(grupo["menor_estoque"]), quantidade)
        grupo["maior_estoque"] = quantidade if grupo["maior_estoque"] is None else max(_to_float(grupo["maior_estoque"]), quantidade)

    resultado = []
    for grupo in grupos.values():
        grupo["quantidade_total_formatada"] = fmt_num(grupo["quantidade_total"])
        grupo["valor_total_estimado_formatado"] = fmt_money(grupo["valor_total_estimado"])
        grupo["menor_estoque_formatado"] = fmt_num(grupo["menor_estoque"] or 0)
        grupo["maior_estoque_formatado"] = fmt_num(grupo["maior_estoque"] or 0)
        resultado.append(grupo)

    resultado.sort(key=lambda item: (-_to_float(item.get("valor_total_estimado")), str(item.get("linha") or "")))
    return resultado[:limite_seguro]


@mcp.tool()
def sugerir_produtos(termo: str, linha: str = "", limite: int = 10) -> list[dict[str, Any]]:
    """Retorna sugestões rápidas de produtos para autocomplete na tela MCP/IA."""
    termo_limpo = str(termo or "").strip()
    if len(termo_limpo) < 2:
        return []
    return buscar_produtos_avancado(termo=termo_limpo, linha=linha, ordenar="descricao", limite=min(_limit(limite, default=10), 20))


@mcp.tool()
def listar_lojas(ativas: bool = True, limite: int = 100) -> list[dict[str, Any]]:
    """Lista lojas cadastradas no sistema."""
    limite_seguro = _limit(limite, default=100)
    where = "WHERE ativo = 1" if ativas else ""
    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(
            f"""
            SELECT id, nome, ativo, criado_em
            FROM stores
            {where}
            ORDER BY nome COLLATE NOCASE ASC
            LIMIT ?
            """,
            (limite_seguro,),
        ).fetchall()
    return _rows_to_dicts(rows)


@mcp.tool()
def listar_lotes_abertos(limite: int = 30) -> list[dict[str, Any]]:
    """Lista lotes/separações ainda não finalizados, agrupando por código de lote."""
    limite_seguro = _limit(limite, default=30)
    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT
                s.lote_codigo,
                s.lote_nome,
                s.data_referencia,
                GROUP_CONCAT(DISTINCT s.status) AS status_encontrados,
                COUNT(DISTINCT s.id) AS total_separacoes,
                GROUP_CONCAT(DISTINCT st.nome) AS lojas,
                COALESCE(SUM(si.quantidade_pedida), 0) AS total_quantidade_pedida,
                COALESCE(SUM(si.quantidade_separada), 0) AS total_quantidade_separada,
                MAX(s.criado_em) AS ultima_atualizacao
            FROM separations s
            JOIN stores st ON st.id = s.store_id
            LEFT JOIN separation_items si ON si.separation_id = s.id
            WHERE s.status <> 'FINALIZADA'
            GROUP BY s.lote_codigo, s.lote_nome, s.data_referencia
            ORDER BY ultima_atualizacao DESC
            LIMIT ?
            """,
            (limite_seguro,),
        ).fetchall()

    return [
        {
            "lote_codigo": row["lote_codigo"],
            "lote_nome": row["lote_nome"],
            "data_referencia": row["data_referencia"],
            "status_encontrados": row["status_encontrados"],
            "total_separacoes": row["total_separacoes"],
            "lojas": row["lojas"],
            "total_quantidade_pedida": _to_float(row["total_quantidade_pedida"]),
            "total_quantidade_separada": _to_float(row["total_quantidade_separada"]),
            "ultima_atualizacao": row["ultima_atualizacao"],
        }
        for row in rows
    ]


@mcp.tool()
def consultar_lote(lote_codigo: str) -> dict[str, Any]:
    """Consulta um lote pelo código e retorna lojas, separações e itens do lote."""
    codigo = str(lote_codigo or "").strip()
    if not codigo:
        return {"encontrado": False, "mensagem": "Informe o código do lote."}

    ensure_default_data()
    with closing(get_conn()) as conn:
        separacoes = conn.execute(
            """
            SELECT
                s.id,
                s.lote_codigo,
                s.lote_nome,
                s.data_referencia,
                s.status,
                s.usar_estoque,
                s.observacao,
                s.criado_em,
                s.enviado_conferencia_em,
                s.finalizado_em,
                st.nome AS loja_nome,
                responsavel.nome AS responsavel_nome,
                conferente.nome AS conferente_nome
            FROM separations s
            JOIN stores st ON st.id = s.store_id
            LEFT JOIN users responsavel ON responsavel.id = s.responsavel_id
            LEFT JOIN users conferente ON conferente.id = s.conferente_id
            WHERE s.lote_codigo = ?
            ORDER BY st.nome COLLATE NOCASE ASC, s.id ASC
            """,
            (codigo,),
        ).fetchall()

        if not separacoes:
            return {"encontrado": False, "lote_codigo": codigo, "mensagem": "Lote não encontrado."}

        separation_ids = [row["id"] for row in separacoes]
        placeholders = ",".join("?" for _ in separation_ids)
        itens = conn.execute(
            f"""
            SELECT
                si.id,
                si.separation_id,
                s.store_id,
                st.nome AS loja_nome,
                si.codigo,
                si.descricao,
                si.fator_embalagem,
                si.quantidade_pedida,
                si.quantidade_separada,
                si.quantidade_conferida,
                si.status,
                si.custo_unitario_ref,
                si.atualizado_em
            FROM separation_items si
            JOIN separations s ON s.id = si.separation_id
            JOIN stores st ON st.id = s.store_id
            WHERE si.separation_id IN ({placeholders})
            ORDER BY st.nome COLLATE NOCASE ASC, si.descricao COLLATE NOCASE ASC
            """,
            separation_ids,
        ).fetchall()

    total_pedido = sum(_to_float(row["quantidade_pedida"]) for row in itens)
    total_separado = sum(_to_float(row["quantidade_separada"]) for row in itens)
    total_conferido = sum(_to_float(row["quantidade_conferida"]) for row in itens)
    valor_estimado = sum(_to_float(row["quantidade_pedida"]) * _to_float(row["custo_unitario_ref"]) for row in itens)

    return {
        "encontrado": True,
        "lote_codigo": codigo,
        "lote_nome": separacoes[0]["lote_nome"],
        "data_referencia": separacoes[0]["data_referencia"],
        "resumo": {
            "total_separacoes": len(separacoes),
            "total_itens": len(itens),
            "total_quantidade_pedida": total_pedido,
            "total_quantidade_separada": total_separado,
            "total_quantidade_conferida": total_conferido,
            "valor_estimado": valor_estimado,
            "valor_estimado_formatado": fmt_money(valor_estimado),
        },
        "separacoes": _rows_to_dicts(separacoes),
        "itens": [
            {
                "id": row["id"],
                "separation_id": row["separation_id"],
                "loja_nome": row["loja_nome"],
                "codigo": row["codigo"],
                "descricao": row["descricao"],
                "fator_embalagem": _to_float(row["fator_embalagem"]) or 1.0,
                "quantidade_pedida": _to_float(row["quantidade_pedida"]),
                "quantidade_separada": _to_float(row["quantidade_separada"]),
                "quantidade_conferida": _to_float(row["quantidade_conferida"]),
                "status": row["status"],
                "custo_unitario_ref": _to_float(row["custo_unitario_ref"]),
                "atualizado_em": row["atualizado_em"],
            }
            for row in itens
        ],
    }


@mcp.tool()
def listar_movimentacoes_estoque(codigo: str = "", tipo: str = "", limite: int = 50) -> list[dict[str, Any]]:
    """Lista o histórico recente de movimentações de estoque, com filtro opcional por produto e tipo."""
    limite_seguro = _limit(limite)
    codigo_limpo = str(codigo or "").strip()
    tipo_limpo = str(tipo or "").strip().upper()

    where = ["1 = 1"]
    params: list[Any] = []

    if codigo_limpo:
        where.append("(si.codigo = ? OR si.codigo_barras = ?)")
        params.extend([codigo_limpo, codigo_limpo])

    if tipo_limpo:
        where.append("sm.tipo = ?")
        params.append(tipo_limpo)

    sql = f"""
        SELECT
            sm.id,
            sm.tipo,
            sm.quantidade,
            sm.observacao,
            sm.referencia_tipo,
            sm.referencia_id,
            sm.criado_em,
            si.codigo,
            si.codigo_barras,
            si.descricao,
            u.nome AS criado_por_nome
        FROM stock_movements sm
        JOIN stock_items si ON si.id = sm.stock_item_id
        LEFT JOIN users u ON u.id = sm.criado_por
        WHERE {' AND '.join(where)}
        ORDER BY sm.id DESC
        LIMIT ?
    """
    params.append(limite_seguro)

    ensure_default_data()
    with closing(get_conn()) as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        {
            "id": row["id"],
            "tipo": row["tipo"],
            "quantidade": _to_float(row["quantidade"]),
            "quantidade_formatada": fmt_num(row["quantidade"]),
            "observacao": row["observacao"],
            "referencia_tipo": row["referencia_tipo"],
            "referencia_id": row["referencia_id"],
            "criado_em": row["criado_em"],
            "codigo": row["codigo"],
            "codigo_barras": row["codigo_barras"],
            "descricao": row["descricao"],
            "criado_por_nome": row["criado_por_nome"],
        }
        for row in rows
    ]




@mcp.tool()
def pesquisar_geral(termo: str, limite: int = 30, linha: str = "") -> dict[str, Any]:
    """Pesquisa o termo em produtos, lojas, lotes e movimentações recentes, podendo filtrar produtos por linha/categoria."""
    termo_limpo = str(termo or "").strip()
    linha_limpa = str(linha or "").strip()
    limite_seguro = _limit(limite, default=30)
    if not termo_limpo:
        return {
            "termo": termo_limpo,
            "linha": linha_limpa,
            "mensagem": "Informe um termo para pesquisar.",
            "produtos": [],
            "lojas": [],
            "lotes": [],
            "movimentacoes": [],
        }

    like = f"%{termo_limpo}%"
    ensure_default_data()
    with closing(get_conn()) as conn:
        produtos = conn.execute(
            """
            SELECT id, codigo, codigo_barras, descricao, fator_embalagem, quantidade_atual, custo_unitario, linha_erp, erp_loja, erp_data_base, atualizado_em
            FROM stock_items
            WHERE ativo = 1
              AND (codigo LIKE ? OR codigo_barras LIKE ? OR descricao LIKE ?)
            ORDER BY descricao COLLATE NOCASE ASC
            LIMIT ?
            """,
            (like, like, like, 5000 if linha_limpa else limite_seguro),
        ).fetchall()

        lojas = conn.execute(
            """
            SELECT id, nome, ativo, criado_em
            FROM stores
            WHERE nome LIKE ?
            ORDER BY nome COLLATE NOCASE ASC
            LIMIT ?
            """,
            (like, min(limite_seguro, 50)),
        ).fetchall()

        lotes = conn.execute(
            """
            SELECT
                s.lote_codigo,
                s.lote_nome,
                s.data_referencia,
                GROUP_CONCAT(DISTINCT s.status) AS status_encontrados,
                COUNT(DISTINCT s.id) AS total_separacoes,
                GROUP_CONCAT(DISTINCT st.nome) AS lojas,
                MAX(s.criado_em) AS ultima_atualizacao
            FROM separations s
            JOIN stores st ON st.id = s.store_id
            WHERE s.lote_codigo LIKE ? OR s.lote_nome LIKE ?
            GROUP BY s.lote_codigo, s.lote_nome, s.data_referencia
            ORDER BY ultima_atualizacao DESC
            LIMIT ?
            """,
            (like, like, min(limite_seguro, 50)),
        ).fetchall()

        movimentacoes = conn.execute(
            """
            SELECT
                sm.id,
                sm.tipo,
                sm.quantidade,
                sm.observacao,
                sm.criado_em,
                si.codigo,
                si.codigo_barras,
                si.descricao,
                u.nome AS criado_por_nome
            FROM stock_movements sm
            JOIN stock_items si ON si.id = sm.stock_item_id
            LEFT JOIN users u ON u.id = sm.criado_por
            WHERE si.codigo LIKE ? OR si.codigo_barras LIKE ? OR si.descricao LIKE ? OR sm.observacao LIKE ?
            ORDER BY sm.id DESC
            LIMIT ?
            """,
            (like, like, like, like, min(limite_seguro, 50)),
        ).fetchall()

    return {
        "termo": termo_limpo,
        "linha": linha_limpa,
        "produtos": listar_produtos_estoque_resultado(produtos, linha=linha_limpa)[:limite_seguro],
        "lojas": _rows_to_dicts(lojas),
        "lotes": _rows_to_dicts(lotes),
        "movimentacoes": [
            {
                "id": row["id"],
                "tipo": row["tipo"],
                "quantidade": _to_float(row["quantidade"]),
                "quantidade_formatada": fmt_num(row["quantidade"]),
                "observacao": row["observacao"],
                "criado_em": row["criado_em"],
                "codigo": row["codigo"],
                "codigo_barras": row["codigo_barras"],
                "descricao": row["descricao"],
                "criado_por_nome": row["criado_por_nome"],
            }
            for row in movimentacoes
        ],
    }


@mcp.resource("alvorada://status")
def resource_status_sistema() -> str:
    """Recurso com o status atual do Sistema Alvorada em JSON."""
    return _json(_status_sistema_data())


@mcp.resource("alvorada://guia")
def resource_guia_mcp() -> str:
    """Guia rápido das ferramentas disponíveis no MCP do Sistema Alvorada."""
    return _json(
        {
            "nome": "Sistema Alvorada MCP",
            "modo": "leitura",
            "ferramentas": [
                "status_sistema",
                "consultar_produto",
                "listar_produtos_estoque",
                "listar_estoque_baixo",
                "buscar_produtos_avancado",
                "resumo_estoque_por_linha",
                "sugerir_produtos",
                "listar_categorias_linha",
                "listar_lojas",
                "listar_lotes_abertos",
                "consultar_lote",
                "listar_movimentacoes_estoque",
                "pesquisar_geral",
            ],
            "exemplos_de_perguntas": [
                "Quais produtos estão com estoque baixo?",
                "Consulte o produto de código 123.",
                "Mostre os lotes abertos.",
                "Analise o lote LT-XXXX e me diga o que falta separar.",
                "Resumo de estoque por linha.",
                "Buscar produtos da linha linguiça com saldo entre 1 e 20.",
            ],
        }
    )


@mcp.prompt()
def analisar_estoque() -> str:
    """Prompt pronto para orientar uma IA a analisar o estoque do Sistema Alvorada."""
    return (
        "Você é um assistente de gestão do Sistema Alvorada. "
        "Use as ferramentas MCP disponíveis para consultar o estoque, identificar produtos com saldo baixo, "
        "comparar quantidades e devolver uma resposta simples, prática e organizada para operação de padaria/estoque. "
        "Não invente dados: quando precisar de números, chame as ferramentas do MCP."
    )


def main() -> None:
    ensure_default_data()
    mcp.run()


if __name__ == "__main__":
    main()
