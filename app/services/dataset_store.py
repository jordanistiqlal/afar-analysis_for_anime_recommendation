import json
import gc

# Kolom bertipe list yang disimpan sebagai JSON string di DB
LIST_COLUMNS = ['studio', 'genre', 'producer', 'keywords', 'theme', 'demographic']

_dataset = None

def _get_connection():
    import os
    from dotenv import load_dotenv

    load_dotenv()

    DB_CONFIG = {
        'host':     os.getenv('DB_HOST', 'localhost'),
        'user':     os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'anime_db'),
        'port':     int(os.getenv('DB_PORT', 3306)),
    }

    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)


def _parse_json_col(value):
    """Kembalikan list dari JSON string yang disimpan di DB."""
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            result = json.loads(value)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass
        # Fallback: comma-separated
        return [i.strip() for i in value.split(',') if i.strip()]
    return []


def _row_to_dict(cursor, row):
    """Ubah tuple row + cursor.description → dict."""
    cols = [desc[0] for desc in cursor.description]
    return dict(zip(cols, row))


def _df_from_rows(rows, cursor):
    import pandas as pd
    records = [_row_to_dict(cursor, r) for r in rows]
    df = pd.DataFrame(records)
    if df.empty:
        return df
    for col in LIST_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(_parse_json_col)
    return df


def get_dataset(use_cache: bool = True):
    global _dataset

    if use_cache and _dataset is not None:
        return _dataset.copy(deep=False)

    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM anime")
    rows   = cursor.fetchall()
    df     = _df_from_rows(rows, cursor)
    cursor.close()
    conn.close()

    if use_cache:
        _dataset = df

    return df.copy(deep=False) if use_cache else df


def get_dataset_filtered(
    columns: list = None,
    where:   str  = None,
    params:  tuple = ()
):
    import pandas as pd

    col_str = ', '.join([f'`{c}`' for c in columns]) if columns else '*'
    sql     = f"SELECT {col_str} FROM anime"
    if where:
        sql += f" WHERE {where}"

    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    rows   = cursor.fetchall()
    df     = _df_from_rows(rows, cursor)
    cursor.close()
    conn.close()
    return df


def get_anime_by_mal_id(mal_id: int):
    conn   = _get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM anime WHERE mal_id = %s LIMIT 1", (mal_id,))
    row    = cursor.fetchone()
    if row is None:
        cursor.close()
        conn.close()
        return None
    result = _row_to_dict(cursor, row)
    cursor.close()
    conn.close()
    for col in LIST_COLUMNS:
        if col in result:
            result[col] = _parse_json_col(result[col])
    return result


def clear_dataset_cache():
    """Reset cache RAM — sama seperti versi CSV."""
    global _dataset
    _dataset = None
    gc.collect()

def upsert_anime_df(df):
    import pandas as pd

    conn   = _get_connection()
    cursor = conn.cursor()

    list_cols = LIST_COLUMNS
    table_cols = [
        'id', 'mal_id', 'title', 'image_url', 'synopsis', 'aired', 'premiered',
        'member', 'favorite', 'source', 'rank', 'link', 'episode', 'type',
        'genre', 'producer', 'studio', 'theme', 'demographic', 'duration',
        'rating', 'mal_score', 'count_user_score', 'keywords',
    ]

    # Rename score → mal_score
    if 'score' in df.columns and 'mal_score' not in df.columns:
        df = df.rename(columns={'score': 'mal_score'})

    # Serialisasi list columns → JSON string
    df = df.copy()
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, list) else (v or '')
            )

    cols_in_df   = [c for c in table_cols if c in df.columns]
    placeholders = ', '.join(['%s'] * len(cols_in_df))
    col_names    = ', '.join([f'`{c}`' for c in cols_in_df])

    # Kolom yang TIDAK di-update saat duplicate (preserve data existing)
    preserve = {'id', 'mal_id', 'rank', 'type', 'producer', 'rating', 'count_user_score'}
    updates  = ', '.join(
        [f'`{c}`=VALUES(`{c}`)' for c in cols_in_df if c not in preserve]
    )

    sql = f"""
        INSERT INTO anime ({col_names})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates}
    """

    batch, BATCH_SIZE = [], 200
    total = 0

    for _, row in df.iterrows():
        values = []
        for col in cols_in_df:
            val = row.get(col)
            if not isinstance(val, str) and pd.isna(val) if hasattr(val, '__float__') else False:
                values.append(None)
            else:
                values.append(val if val != '' else None)
        batch.append(tuple(values))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            batch = []

    if batch:
        cursor.executemany(sql, batch)
        conn.commit()
        total += len(batch)

    cursor.close()
    conn.close()

    clear_dataset_cache()
    return total