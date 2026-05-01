"""
migrate_to_sql.py
-----------------
Jalankan SEKALI untuk migrasi dataset.csv → MySQL.
Usage: python migrate_to_sql.py
"""

import pandas as pd
import mysql.connector
import ast
import os
from dotenv import load_dotenv

load_dotenv()

# ── Konfigurasi MySQL ──────────────────────────────────────────────────────────
DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'user':     os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'anime_db'),
    'port':     int(os.getenv('DB_PORT', 3306)),
}

CSV_PATH = './app/static/data/dataset.csv'

# ── DDL ────────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS anime (
    id              VARCHAR(36)   PRIMARY KEY,
    mal_id          INT           UNIQUE,
    title           VARCHAR(512),
    image_url       TEXT,
    synopsis        LONGTEXT,
    aired           VARCHAR(128),
    premiered       VARCHAR(64),
    member          INT,
    favorite        INT           DEFAULT 0,
    source          VARCHAR(128),
    `rank`          VARCHAR(64),
    link            TEXT,
    episode         VARCHAR(64),
    type            VARCHAR(64),
    genre           TEXT,
    producer        TEXT,
    studio          TEXT,
    theme           TEXT,
    demographic     TEXT,
    duration        VARCHAR(64),
    rating          VARCHAR(128),
    mal_score       FLOAT,
    count_user_score FLOAT,
    keywords        TEXT,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_mal_id   (mal_id),
    INDEX idx_score    (mal_score),
    INDEX idx_premiered(premiered)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

# ── Helpers ────────────────────────────────────────────────────────────────────
def parse_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except Exception:
            return [i.strip() for i in x.split(',') if i.strip()]
    return []

def list_to_str(x):
    """Simpan list sebagai string JSON-like supaya mudah di-parse balik."""
    import json
    if isinstance(x, list):
        return json.dumps(x, ensure_ascii=False)
    return x or ''

def load_csv(path):
    df = pd.read_csv(path)
    df['mal_id'] = pd.to_numeric(df['mal_id'], errors='coerce')

    # Rename score → mal_score kalau belum
    if 'score' in df.columns and 'mal_score' not in df.columns:
        df = df.rename(columns={'score': 'mal_score'})

    list_cols = ['studio', 'genre', 'producer', 'keywords', 'theme', 'demographic']
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(parse_list).apply(list_to_str)

    return df

# ── Main ───────────────────────────────────────────────────────────────────────
def migrate():
    print("📂  Membaca CSV …")
    df = load_csv(CSV_PATH)
    print(f"    {len(df)} baris ditemukan.")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("🗄️   Membuat tabel (jika belum ada) …")
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()

    # Kolom yang ada di tabel
    table_cols = [
        'id', 'mal_id', 'title', 'image_url', 'synopsis', 'aired', 'premiered',
        'member', 'favorite', 'source', 'rank', 'link', 'episode', 'type',
        'genre', 'producer', 'studio', 'theme', 'demographic', 'duration',
        'rating', 'mal_score', 'count_user_score', 'keywords',
    ]

    # Hanya ambil kolom yang ada di df
    cols_in_df = [c for c in table_cols if c in df.columns]
    placeholders = ', '.join(['%s'] * len(cols_in_df))
    col_names    = ', '.join([f'`{c}`' for c in cols_in_df])
    updates      = ', '.join([f'`{c}`=VALUES(`{c}`)' for c in cols_in_df if c not in ('id', 'mal_id')])

    sql = f"""
        INSERT INTO anime ({col_names})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates}
    """

    print("⬆️   Memasukkan data ke MySQL …")
    batch, BATCH_SIZE = [], 500
    inserted = 0

    for _, row in df.iterrows():
        values = []
        for col in cols_in_df:
            val = row.get(col)
            # Konversi NaN → None
            if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                values.append(None)
            else:
                values.append(val)
        batch.append(tuple(values))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
            print(f"    … {inserted} baris selesai")
            batch = []

    if batch:
        cursor.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)

    print(f"✅  Migrasi selesai — total {inserted} baris.")
    cursor.close()
    conn.close()

if __name__ == '__main__':
    migrate()