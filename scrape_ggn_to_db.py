import json
import requests
import time
import re
import html
import sqlite3

# ===================== CONFIG =====================

TORRENT_LIMIT = 20  # limit for testing; set to None for all

API_URL = 'https://gazellegames.net/api.php'
REQUEST_STRING = '?request='
SEARCH_STRING = REQUEST_STRING + 'search&search_type=torrents'

SPECIAL_EDITIONS = ["redump"]
USA_REGIONS = ["USA", "NTSC"]
EUR_REGIONS = ["Europe", "PAL", "PAL-E"]
JPN_REGIONS = ["Japan", "NTSC-J"]

DB_PATH = "ggn_redump.db"

# ===================== SECRETS =====================

with open('secrets.json', 'r') as f:
    objSecrets = json.load(f)
API_KEY = objSecrets['api_key']
HEADER = {"x-api-key": API_KEY}

# ===================== DB SETUP =====================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Main torrent table (includes redump_id from GGn description)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS torrents (
            torrent_id     INTEGER PRIMARY KEY,
            group_id       INTEGER,
            group_name     TEXT,
            group_year     INTEGER,
            info_hash      TEXT,
            region         TEXT,
            language       TEXT,
            remaster_year  INTEGER,
            remaster_title TEXT,
            release_title  TEXT,
            release_type   TEXT,
            redump_id      INTEGER
        )
    """)

    # Files per torrent
    cur.execute("""
        CREATE TABLE IF NOT EXISTS torrent_files (
            torrent_id  INTEGER,
            file_index  INTEGER,
            file_name   TEXT,
            PRIMARY KEY (torrent_id, file_index),
            FOREIGN KEY (torrent_id) REFERENCES torrents(torrent_id)
        )
    """)

    conn.commit()
    return conn

# ===================== HELPERS =====================

def extract_redump_nr(torrent):
    """Extract Redump disc ID from GGn torrent description or bbDescription."""
    text = torrent.get("description") or torrent.get("bbDescription") or ""
    if not text:
        return None

    text = html.unescape(text)

    # Explicit redump.org/disc/12345 style
    m = re.search(r"redump\.org[\\/]+disc[\\/]+(\d+)", text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None

    # Fallback: generic disc/12345
    m = re.search(r"disc[\\/]+(\d+)", text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None

    return None

# ===================== GGn LOGIC =====================

def generate_list(console_torrents):
    torrents_to_fetch = []
    for group, groupData in console_torrents.items():
        if 'Torrents' in groupData and groupData['Torrents']:
            for torrent, torrentData in groupData['Torrents'].items():
                if (
                    torrentData['RemasterTitle'] != "" and
                    any(term in torrentData['RemasterTitle'].lower() for term in SPECIAL_EDITIONS)
                ):
                    if targetRegion:
                        if any(torrentData['Region'] in region for region in targetRegion):
                            torrents_to_fetch.append(torrentData['ID'])
                    else:
                        torrents_to_fetch.append(torrentData['ID'])
    print(f"Number of torrents to fetch on this page: {len(torrents_to_fetch)}")
    return torrents_to_fetch


def fetch_pages(session):
    final_torrents = []
    target_page = 1

    while True:
        constructed_request = f"{API_URL}{SEARCH_STRING}&page={target_page}&artistname={targetConsole}"
        response = session.get(constructed_request, headers=HEADER)
        data = response.json()

        if not data.get('response'):
            break

        console_torrents = data['response']
        new_ids = generate_list(console_torrents)
        final_torrents += new_ids

        print(f"Fetched page {target_page}, total so far: {len(final_torrents)}")

        if TORRENT_LIMIT and len(final_torrents) >= TORRENT_LIMIT:
            final_torrents = final_torrents[:TORRENT_LIMIT]
            print(f"Reached limit of {TORRENT_LIMIT} torrents — stopping early.")
            break

        target_page += 1
        time.sleep(2.1)  # respect rate limit

    print(f"Total torrents collected: {len(final_torrents)}")
    return final_torrents


def store_torrent_in_db(conn, group, torrent):
    cur = conn.cursor()

    # Clean weird HTML entities
    group_name = html.unescape(group.get('name', ''))
    release_title = html.unescape(torrent.get('releaseTitle', ''))
    remaster_title = html.unescape(torrent.get('remasterTitle', ''))

    redump_id = extract_redump_nr(torrent)

    # Insert / update torrent row
    cur.execute("""
        INSERT OR REPLACE INTO torrents (
            torrent_id, group_id, group_name, group_year,
            info_hash, region, language,
            remaster_year, remaster_title,
            release_title, release_type, redump_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        torrent.get('id'),
        group.get('id'),
        group_name,
        group.get('year'),
        torrent.get('infoHash'),
        torrent.get('region'),
        torrent.get('language'),
        torrent.get('remasterYear'),
        remaster_title,
        release_title,
        torrent.get('releaseType'),
        redump_id
    ))

    # Reset file list for this torrent
    cur.execute("DELETE FROM torrent_files WHERE torrent_id = ?", (torrent.get('id'),))

    files = torrent.get('fileList', [])
    for file_index, f in enumerate(files, start=1):
        clean_filename = html.unescape(f.get('name', ''))
        cur.execute("""
            INSERT OR REPLACE INTO torrent_files (
                torrent_id, file_index, file_name
            ) VALUES (?, ?, ?)
        """, (
            torrent.get('id'),
            file_index,
            clean_filename
        ))

    conn.commit()


def collect_torrent_metadata(conn, torrent_ids, session):
    for index, tid in enumerate(torrent_ids):
        print(f"Fetching torrent metadata {index+1}/{len(torrent_ids)} (ID: {tid})")
        url = f"{API_URL}?request=torrent&id={tid}"
        resp = session.get(url, headers=HEADER)

        try:
            data = resp.json()
        except Exception as e:
            print(f"Error decoding JSON for torrent {tid}: {e}")
            continue

        if data.get("status") != "success":
            print(f"Skipping {tid}, bad response: {data.get('status')}")
            continue

        group = data["response"]["group"]
        torrent = data["response"]["torrent"]

        store_torrent_in_db(conn, group, torrent)
        time.sleep(2.1)

    print("\n✅ Metadata collection complete. Data stored in SQLite DB:", DB_PATH)

# ===================== MAIN =====================

if __name__ == "__main__":
    targetConsole = input("Target Console: ")
    targetRegion = input("Target Region (Japan, Europe, USA, empty for All): ")
    if targetRegion == "Japan":
        targetRegion = JPN_REGIONS
    elif targetRegion == "Europe":
        targetRegion = EUR_REGIONS
    elif targetRegion == "USA":
        targetRegion = USA_REGIONS
    else:
        targetRegion = ""

    print(f"Target Console: {targetConsole}")
    print(f"Target Region: {targetRegion}")

    conn = init_db()
    session = requests.Session()

    torrent_ids = fetch_pages(session)
    collect_torrent_metadata(conn, torrent_ids, session)

    conn.close()
    print("\n🎉 Done. Torrents + files stored in", DB_PATH)
