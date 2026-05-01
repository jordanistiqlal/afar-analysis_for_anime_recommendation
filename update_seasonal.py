#!/usr/bin/env python3
"""
update_seasonal.py
------------------
Script mandiri untuk cronjob — tidak perlu Jupyter.
Otomatis mendeteksi season saat ini lalu scrape MAL dan upsert ke MySQL.

Cara pakai:
    python update_seasonal.py               # auto-detect season
    python update_seasonal.py 2026 spring   # override manual

Contoh crontab (setiap Senin jam 03:00):
    0 3 * * 1 /usr/bin/python3 /home/username/scripts/update_seasonal.py >> /home/username/logs/anime_update.log 2>&1
"""

import sys
import os
import uuid
import json
import time
import logging
from datetime import datetime

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Env & DB ───────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # dotenv opsional

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'user':     os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'anime_db'),
    'port':     int(os.getenv('DB_PORT', 3306)),
}

# ── Season detection ───────────────────────────────────────────────────────────
def current_season():
    month = datetime.now().month
    year  = datetime.now().year
    if month in (1, 2, 3):   return str(year), 'winter'
    if month in (4, 5, 6):   return str(year), 'spring'
    if month in (7, 8, 9):   return str(year), 'summer'
    return str(year), 'fall'

# ── Scraper ────────────────────────────────────────────────────────────────────
def scrape_seasonal(year, season):
    from bs4 import BeautifulSoup
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    import pandas as pd

    log.info(f'Membuka browser → MAL season/{year}/{season}')
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')

    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=options)

    driver.get(f'https://myanimelist.net/anime/season/{year}/{season}')

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, 'content'))
        )
    except Exception:
        log.warning('Timeout menunggu halaman — melanjutkan dengan konten yang ada')

    soup  = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()
    items = soup.find_all('div', class_='js-anime-type-all')
    log.info(f'Ditemukan {len(items)} item')

    def ensure_list(v):
        if isinstance(v, list): return v
        if isinstance(v, str) and v: return [v]
        return []

    data = []
    for j in items:
        try:
            mal_id   = int((j.find('a', class_='link-title').get('href')).split('/')[4])
            title    = j.find('span', class_='js-title').text
            link     = j.find('a', class_='link-title').get('href')
            score_s  = j.find('span', class_='js-score').text
            member_s = j.find('span', class_='js-members').text
            synopsis = j.find('p', class_='preline').text
            image    = j.find('div', class_='image').a['href']
            genres   = [g.a.text.strip() for g in j.find_all('span', class_='genre')]

            aired = (el.text.strip() if (el := j.select_one('div.prodsrc div.info span.item')) else '')
            info_text = (j.select_one('div.prodsrc div.info').text if j.select_one('div.prodsrc div.info') else '')
            episode, duration = (info_text.replace(aired, '').replace('\n', '').replace(' ', '').split(',') + ['', ''])[:2]

            props = {}
            for prop in j.find_all('div', class_='property'):
                cap = prop.find('span', class_='caption')
                if not cap: continue
                vals = [i.get_text(strip=True) for i in prop.find_all('span', class_='item')]
                props[cap.text.strip()] = vals if len(vals) > 1 else (vals[0] if vals else '')

            studio      = props.get('Studios') or props.get('Studio') or []
            source      = props.get('Sources') or props.get('Source') or ''
            themes      = props.get('Themes')  or props.get('Theme')  or []
            demographic = props.get('Demographics') or props.get('Demographic') or []

            data.append({
                'id':               str(uuid.uuid4()),
                'mal_id':           mal_id,
                'title':            title,
                'image_url':        image,
                'synopsis':         synopsis,
                'aired':            aired,
                'premiered':        f'{season} {year}',
                'member':           int(member_s.replace(',', '')),
                'favorite':         0,
                'source':           source if isinstance(source, str) else ','.join(source),
                'rank':             '',
                'link':             link,
                'episode':          episode,
                'type':             '',
                'genre':            genres,
                'producer':         [],
                'studio':           ensure_list(studio),
                'theme':            ensure_list(themes),
                'demographic':      ensure_list(demographic),
                'duration':         duration,
                'rating':           '',
                'mal_score':        float(score_s) if score_s.replace('.','').isdigit() else 0.0,
                'count_user_score': 0.0,
            })
        except Exception as e:
            log.warning(f'Skip satu item karena error: {e}')
            continue

    return pd.DataFrame(data)

# ── Upsert ─────────────────────────────────────────────────────────────────────
def upsert_to_db(df):
    import mysql.connector
    import pandas as pd

    LIST_COLS  = ['studio', 'genre', 'producer', 'keywords', 'theme', 'demographic']
    TABLE_COLS = [
        'id', 'mal_id', 'title', 'image_url', 'synopsis', 'aired', 'premiered',
        'member', 'favorite', 'source', 'rank', 'link', 'episode', 'type',
        'genre', 'producer', 'studio', 'theme', 'demographic', 'duration',
        'rating', 'mal_score', 'count_user_score',
    ]
    PRESERVE = {'id', 'mal_id', 'rank', 'type', 'producer', 'rating', 'count_user_score'}

    df = df.copy()
    for col in LIST_COLS:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, list) else (v or '')
            )

    cols_in_df   = [c for c in TABLE_COLS if c in df.columns]
    placeholders = ', '.join(['%s'] * len(cols_in_df))
    col_names    = ', '.join([f'`{c}`' for c in cols_in_df])
    updates      = ', '.join([f'`{c}`=VALUES(`{c}`)' for c in cols_in_df if c not in PRESERVE])

    sql = f"INSERT INTO anime ({col_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"

    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    batch, BATCH_SIZE, total = [], 200, 0

    for _, row in df.iterrows():
        values = []
        for col in cols_in_df:
            val = row.get(col, None)
            values.append(None if (val is None or (not isinstance(val, str) and pd.isna(val))) else val)
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
    return total

# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) == 3:
        year, season = sys.argv[1], sys.argv[2].lower()
    else:
        year, season = current_season()

    log.info(f'=== Update Seasonal: {season} {year} ===')
    start = time.time()

    df = scrape_seasonal(year, season)
    if df.empty:
        log.error('Tidak ada data yang berhasil di-scrape. Keluar.')
        sys.exit(1)

    n = upsert_to_db(df)
    elapsed = time.time() - start
    log.info(f'=== Selesai: {n} baris di-upsert dalam {elapsed:.1f}s ===')

if __name__ == '__main__':
    main()