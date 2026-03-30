from __future__ import annotations

from collections.abc import Iterable

import oracledb

from app.config import settings


def _init_client() -> None:
    try:
        oracledb.init_oracle_client(lib_dir=settings.oracle_client_dir)
    except Exception:
        # Em alguns ambientes Linux com thin mode, init não é obrigatório.
        pass


def fetch_rows(sql: str) -> list[dict]:
    _init_client()
    conn = oracledb.connect(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=settings.oracle_dsn,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [col[0].lower() for col in cursor.description]
            rows = cursor.fetchall()
            normalized_rows: list[dict] = []
            for row in rows:
                normalized_row: list[object] = []
                for value in row:
                    if isinstance(value, oracledb.LOB):
                        try:
                            normalized_row.append(value.read())
                        except Exception:
                            normalized_row.append("<LOB indisponível>")
                    else:
                        normalized_row.append(value)
                normalized_rows.append(dict(zip(columns, normalized_row, strict=False)))
            return normalized_rows
    finally:
        conn.close()


def safe_first_number(rows: Iterable[dict], *keys: str) -> float:
    for row in rows:
        for key in keys:
            value = row.get(key)
            if isinstance(value, (int, float)):
                return float(value)
    return 0.0
