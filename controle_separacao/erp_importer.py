from __future__ import annotations

import io
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from typing import Any

NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

HEADER_ALIASES = {
    "codigo": {"codigo item", "codigo", "cod item", "cod produto", "codigo produto"},
    "codigo_barras": {"codigo barras", "cod barras", "codigo de barras", "ean", "gtin"},
    "nivel": {"nivel", "nível"},
    "descricao": {"descricao", "descrição", "produto", "nome produto"},
    "preco_custo": {"preco custo", "preço custo", "custo", "valor custo"},
    "preco_venda": {"preco venda", "preço venda", "venda", "valor venda"},
    "saldo_qtd": {"saldo qtd", "saldo quantidade", "quantidade", "qtd", "saldo"},
    "saldo_custo": {"saldo custo"},
    "saida_custo": {"saida custo", "saída custo"},
    "saldo_venda": {"saldo venda"},
    "saida_venda": {"saida venda", "saída venda"},
    "dias": {"dias"},
    "sugestao": {"sugestao", "sugestão"},
    "estoque_ideal": {"est ideal", "estoque ideal", "est. ideal"},
}


def _normalizar(texto: Any) -> str:
    value = unicodedata.normalize("NFKD", str(texto or ""))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.casefold()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def _col_index(col: str) -> int:
    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch.upper()) - 64)
    return idx


def _cell_value(cell: ET.Element, shared: list[str]) -> Any:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        texts = [t.text or "" for t in cell.findall(".//main:t", NS)]
        return "".join(texts)
    value_el = cell.find("main:v", NS)
    if value_el is None:
        return None
    value = value_el.text or ""
    if cell_type == "s":
        try:
            return shared[int(value)]
        except (ValueError, IndexError):
            return value
    if cell_type == "b":
        return value == "1"
    return value


def _shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    result: list[str] = []
    for si in root.findall("main:si", NS):
        result.append("".join(t.text or "" for t in si.findall(".//main:t", NS)))
    return result


def _read_xlsx_like(file_bytes: bytes) -> list[list[Any]]:
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        shared = _shared_strings(zf)
        sheet_names = [name for name in zf.namelist() if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")]
        if not sheet_names:
            raise ValueError("Nenhuma planilha foi encontrada dentro do arquivo.")
        root = ET.fromstring(zf.read(sheet_names[0]))
        rows: list[list[Any]] = []
        for row in root.findall(".//main:sheetData/main:row", NS):
            values_by_col: dict[int, Any] = {}
            max_col = 0
            for cell in row.findall("main:c", NS):
                ref = cell.attrib.get("r", "")
                match = re.match(r"([A-Z]+)(\d+)", ref)
                col = _col_index(match.group(1)) if match else max_col + 1
                max_col = max(max_col, col)
                values_by_col[col] = _cell_value(cell, shared)
            if max_col:
                rows.append([values_by_col.get(i) for i in range(1, max_col + 1)])
        return rows


def _read_legacy_xls(file_bytes: bytes) -> list[list[Any]]:
    try:
        import xlrd
    except ImportError as exc:
        raise ValueError("Arquivo .xls antigo detectado. Instale a dependência xlrd ou exporte o relatório como Excel .xlsx.") from exc
    book = xlrd.open_workbook(file_contents=file_bytes)
    sheet = book.sheet_by_index(0)
    return [[sheet.cell_value(r, c) for c in range(sheet.ncols)] for r in range(sheet.nrows)]


def ler_planilha_erp(file_bytes: bytes) -> list[list[Any]]:
    if file_bytes[:2] == b"PK":
        return _read_xlsx_like(file_bytes)
    return _read_legacy_xls(file_bytes)


def _texto(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).replace("\xa0", " ").strip()
    if text.endswith(".0") and re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    return text


def _numero(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("\xa0", " ")
    if not text:
        return 0.0
    text = text.replace("R$", "").strip()
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    text = re.sub(r"[^0-9eE+\-.]", "", text)
    try:
        return float(text)
    except ValueError:
        return 0.0


def _mapear_cabecalho(row: list[Any]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    normalized_cells = [_normalizar(cell) for cell in row]
    for field, aliases in HEADER_ALIASES.items():
        normalized_aliases = {_normalizar(alias) for alias in aliases}
        for idx, cell in enumerate(normalized_cells):
            if cell in normalized_aliases:
                mapping[field] = idx
                break
    return mapping


def _encontrar_cabecalho(rows: list[list[Any]]) -> tuple[int, dict[str, int]]:
    for idx, row in enumerate(rows[:80]):
        mapping = _mapear_cabecalho(row)
        if {"codigo", "descricao", "saldo_qtd"}.issubset(mapping):
            return idx, mapping
    raise ValueError("Não encontrei o cabeçalho do relatório. Preciso das colunas Código Item, Descrição e Saldo Qtd.")


def _extrair_data_base(rows: list[list[Any]], limite: int) -> str:
    for row in rows[:limite]:
        line = " ".join(_texto(cell) for cell in row if _texto(cell))
        match = re.search(r"DATA\s*BASE\s*:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})", line, flags=re.I)
        if match:
            return match.group(1)
    return ""


def _extrair_loja(rows: list[list[Any]]) -> str:
    for row in rows[:5]:
        line = " ".join(_texto(cell) for cell in row if _texto(cell))
        match = re.search(r"DETALHE\s+DA\s+LOJA\s+(.+)$", line, flags=re.I)
        if match:
            return match.group(1).strip()
    return ""


def _nivel_depth(nivel: str) -> int:
    clean = _texto(nivel)
    if not clean:
        return 0
    return len([part for part in clean.split(".") if part != ""])


def _is_produto(codigo: str, codigo_barras: str, descricao: str) -> bool:
    if not codigo or not descricao:
        return False
    digits_code = re.fullmatch(r"\d+", codigo or "") is not None
    digits_bar = re.fullmatch(r"\d+", codigo_barras or "") is not None
    return (digits_code and len(codigo) >= 3) or (digits_bar and len(codigo_barras) >= 6)


def _get(row: list[Any], mapping: dict[str, int], field: str) -> Any:
    idx = mapping.get(field)
    if idx is None or idx >= len(row):
        return None
    return row[idx]


def parse_erp_stock_file(file_bytes: bytes, filename: str = "") -> dict[str, Any]:
    rows = ler_planilha_erp(file_bytes)
    header_idx, mapping = _encontrar_cabecalho(rows)
    loja = _extrair_loja(rows)
    data_base = _extrair_data_base(rows, header_idx)
    categorias: dict[int, str] = {}
    produtos: list[dict[str, Any]] = []
    grupos = 0
    ignorados = 0

    for row_number, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        codigo = _texto(_get(row, mapping, "codigo"))
        codigo_barras = _texto(_get(row, mapping, "codigo_barras"))
        nivel = _texto(_get(row, mapping, "nivel"))
        descricao_original = _texto(_get(row, mapping, "descricao"))
        descricao = " ".join(descricao_original.split())
        if not any(_texto(cell) for cell in row):
            continue
        depth = _nivel_depth(nivel)
        if not _is_produto(codigo, codigo_barras, descricao):
            if descricao:
                if depth <= 0:
                    depth = max(categorias.keys(), default=0) + 1
                categorias = {k: v for k, v in categorias.items() if k < depth}
                categorias[depth] = descricao.title()
                grupos += 1
            else:
                ignorados += 1
            continue
        parent_depth = max(depth - 1, 0)
        linha = categorias.get(parent_depth) or (categorias[max(categorias.keys())] if categorias else "Sem categoria")
        caminho = " / ".join(categorias[k] for k in sorted(categorias) if categorias[k])
        produtos.append({
            "row_number": row_number,
            "codigo": codigo,
            "codigo_barras": codigo_barras,
            "nivel": nivel,
            "descricao": descricao,
            "linha": linha,
            "caminho_linha": caminho,
            "preco_custo": _numero(_get(row, mapping, "preco_custo")),
            "preco_venda": _numero(_get(row, mapping, "preco_venda")),
            "saldo_qtd": _numero(_get(row, mapping, "saldo_qtd")),
            "saldo_custo": _numero(_get(row, mapping, "saldo_custo")),
            "saida_custo": _numero(_get(row, mapping, "saida_custo")),
            "saldo_venda": _numero(_get(row, mapping, "saldo_venda")),
            "saida_venda": _numero(_get(row, mapping, "saida_venda")),
            "dias": _numero(_get(row, mapping, "dias")),
            "sugestao": _numero(_get(row, mapping, "sugestao")),
            "estoque_ideal": _numero(_get(row, mapping, "estoque_ideal")),
        })

    dedup: dict[str, dict[str, Any]] = {}
    duplicados = 0
    for item in produtos:
        if item["codigo"] in dedup:
            duplicados += 1
        dedup[item["codigo"]] = item
    produtos = list(dedup.values())

    return {
        "filename": filename,
        "loja": loja,
        "data_base": data_base,
        "header_row": header_idx + 1,
        "total_linhas": len(rows),
        "total_grupos": grupos,
        "total_produtos": len(produtos),
        "duplicados": duplicados,
        "ignorados": ignorados,
        "produtos": produtos,
    }
