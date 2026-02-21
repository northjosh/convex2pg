# convex2pg

Convert a [Convex](https://convex.dev) snapshot export to PostgreSQL `schema.sql` + `data.sql` files.

Zero dependencies — standard library only.

## Install

```bash
pip install -e ~/dev/convex2pg

# or with pipx (recommended for CLI tools)
pipx install ~/dev/convex2pg
```

## Usage

```bash
# Write schema.sql + data.sql into the export directory
convex2pg ./my-convex-export/

# Write to a different output directory
convex2pg ./my-convex-export/ --output ./sql/

# Schema only (no inserts)
convex2pg ./my-convex-export/ --schema-only

# Data only (no CREATE TABLE)
convex2pg ./my-convex-export/ --data-only

# Print to stdout (pipe into psql)
convex2pg ./my-convex-export/ --stdout | psql -U user -d mydb

# Show per-table details
convex2pg ./my-convex-export/ --verbose
```

## Convex export format

A Convex snapshot export is a directory containing:

```
export/
├── _tables/documents.jsonl        # table metadata (skipped)
├── <table>/
│   ├── documents.jsonl            # one JSON object per line
│   └── generated_schema.jsonl    # Convex type info
└── _components/
    └── <component>/
        └── <table>/
            ├── documents.jsonl
            └── generated_schema.jsonl
```

## Type mapping

| Convex type      | PostgreSQL type    |
|------------------|--------------------|
| `normalfloat64`  | `DOUBLE PRECISION` |
| `normalint64`    | `BIGINT`           |
| `field_name`     | `TEXT`             |
| `boolean`        | `BOOLEAN`          |
| `array`/`object` | `JSONB`            |
| Convex ID (`_id`, `*Id` fields) | `VARCHAR(50)` |
| Timestamp fields (`*At`, `*Time`, `expires*`) | `BIGINT` (ms since epoch) |

Timestamps are stored as milliseconds since epoch (`BIGINT`).
Convert to `TIMESTAMP` in PostgreSQL with:

```sql
SELECT to_timestamp(created_at / 1000.0) FROM "user";
```

## Load into PostgreSQL

```bash
psql -U youruser -d yourdb -f schema.sql
psql -U youruser -d yourdb -f data.sql
```
### This was written entirely by Claude