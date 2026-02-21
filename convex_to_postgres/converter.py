"""
Core conversion logic: Convex JSONL export → PostgreSQL SQL.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


# ── Type mappings ─────────────────────────────────────────────────────────────

CONVEX_TYPE_MAP: dict[str, str] = {
    "normalfloat64": "DOUBLE PRECISION",
    "normalint64":   "BIGINT",
    "int64":         "BIGINT",
    "field_name":    "TEXT",
    "boolean":       "BOOLEAN",
    "string":        "TEXT",
    "bytes":         "BYTEA",
    "any":           "JSONB",
    "array":         "JSONB",
    "object":        "JSONB",
    "null":          "TEXT",
}

# Field name substrings that indicate millisecond timestamp storage
TIMESTAMP_HINTS = ("_at", "_time", "expires", "timestamp")

# Convex IDs are alphanumeric strings, typically 32 chars
_CONVEX_ID_RE = re.compile(r"^[a-zA-Z0-9]{20,}$")


# ── Helpers ───────────────────────────────────────────────────────────────────

def camel_to_snake(name: str) -> str:
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()


def is_convex_id(value: str) -> bool:
    return bool(_CONVEX_ID_RE.match(value))


def is_timestamp_field(snake_name: str) -> bool:
    return any(h in snake_name for h in TIMESTAMP_HINTS)


def infer_pg_type(field_name: str, value: Any) -> str:
    """Infer PostgreSQL column type from a live Python value."""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "BIGINT" if value > 1e12 else "DOUBLE PRECISION"
    if isinstance(value, str):
        if field_name == "_id" or (field_name.endswith("Id") and is_convex_id(value)):
            return "VARCHAR(50)"
        return "TEXT"
    if isinstance(value, (dict, list)):
        return "JSONB"
    return "TEXT"


def escape_value(snake_name: str, value: Any) -> str:
    """Return a SQL-safe literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        return str(int(value)) if (is_timestamp_field(snake_name) or value > 1e12) else repr(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    # dict / list → JSONB
    return "'" + json.dumps(value).replace("'", "''") + "'"


# ── Schema parsing ────────────────────────────────────────────────────────────

def _parse_convex_schema(schema_file: Path) -> dict[str, str]:
    """
    Parse Convex's generated_schema.jsonl.

    The file contains a JSON-encoded string whose inner content uses bare
    (unquoted) type identifiers like `normalfloat64` or `field_name` mixed
    with quoted example values for string/ID fields.
    """
    try:
        inner: str = json.loads(schema_file.read_text().strip())
        fields: dict[str, str] = {}
        for m in re.finditer(r'"([^"]+)"\s*:\s*(?:"([^"]*)"|([\w]+))', inner):
            name, quoted_val, type_id = m.group(1), m.group(2), m.group(3)
            if type_id:
                fields[name] = CONVEX_TYPE_MAP.get(type_id, "TEXT")
            else:
                fields[name] = "VARCHAR(50)" if (quoted_val and is_convex_id(quoted_val)) else "TEXT"
        return fields
    except Exception:
        return {}


def _infer_schema_from_docs(docs_file: Path) -> dict[str, str]:
    """Scan documents to infer column types when no schema file is available."""
    fields: dict[str, str] = {}
    with open(docs_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            for k, v in json.loads(line).items():
                if k not in fields and v is not None:
                    fields[k] = infer_pg_type(k, v)
    return fields


# ── Table processing ──────────────────────────────────────────────────────────

class TableResult:
    def __init__(self, name: str, schema_sql: str, data_sql: str, row_count: int):
        self.name = name
        self.schema_sql = schema_sql
        self.data_sql = data_sql
        self.row_count = row_count


def convert_table(table_name: str, table_dir: Path) -> TableResult | None:
    """Convert one Convex table directory into SQL strings."""
    docs_file = table_dir / "documents.jsonl"
    if not docs_file.exists():
        return None

    # Determine column types
    pg_types: dict[str, str] = {}
    schema_file = table_dir / "generated_schema.jsonl"
    if schema_file.exists():
        pg_types = _parse_convex_schema(schema_file)
    if not pg_types:
        pg_types = _infer_schema_from_docs(docs_file)
    if not pg_types:
        return None

    # Load documents
    docs: list[dict] = []
    with open(docs_file) as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))

    # Build column definitions + name map
    col_defs: list[str] = []
    col_map: dict[str, str] = {}  # original → snake_case

    for orig, pg_type in pg_types.items():
        if orig == "_id":
            snake = "id"
            col_defs.append("  id VARCHAR(50) PRIMARY KEY")
        elif orig == "_creationTime":
            snake = "creation_time"
            col_defs.append("  creation_time BIGINT")
        else:
            snake = camel_to_snake(orig)
            if is_timestamp_field(snake):
                pg_type = "BIGINT"
            col_defs.append(f"  {snake} {pg_type}")
        col_map[orig] = snake

    # Schema SQL
    schema_sql = (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        + ",\n".join(col_defs)
        + "\n);"
    )

    # Data SQL
    insert_lines: list[str] = []
    for doc in docs:
        cols, vals = [], []
        for orig, snake in col_map.items():
            value = doc.get(orig)
            if isinstance(value, float) and (is_timestamp_field(snake) or orig == "_creationTime"):
                value = int(value)
            cols.append(snake)
            vals.append(escape_value(snake, value))
        insert_lines.append(
            f"INSERT INTO {table_name} ({', '.join(cols)}) "
            f"VALUES ({', '.join(vals)}) "
            f"ON CONFLICT (id) DO NOTHING;"
        )

    return TableResult(
        name=table_name,
        schema_sql=schema_sql,
        data_sql="\n".join(insert_lines),
        row_count=len(docs),
    )


# ── Export directory traversal ────────────────────────────────────────────────

def convert_export(export_dir: Path) -> list[TableResult]:
    """
    Walk a Convex snapshot export and convert every table found.

    Handles both root-level tables and component tables
    (under _components/<component>/<table>).
    """
    results: list[TableResult] = []

    def _skip(name: str) -> bool:
        return name.startswith(("_", "."))

    # Root-level tables
    for entry in sorted(export_dir.iterdir()):
        if entry.is_dir() and not _skip(entry.name):
            r = convert_table(entry.name, entry)
            if r:
                results.append(r)

    # Component tables: _components/<component>/<table>
    components_dir = export_dir / "_components"
    if components_dir.exists():
        for component in sorted(components_dir.iterdir()):
            if not component.is_dir() or _skip(component.name):
                continue
            for table_dir in sorted(component.iterdir()):
                if table_dir.is_dir() and not _skip(table_dir.name):
                    r = convert_table(table_dir.name, table_dir)
                    if r:
                        results.append(r)

    return results
