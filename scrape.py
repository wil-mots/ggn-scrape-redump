import json
import os
import requests
import time
import csv
import re
import html

TORRENT_LIMIT = 5  # Limit how many torrents to process (for testing)

with open('secrets.json', 'r') as f:
    objSecrets = json.load(f)
API_KEY = objSecrets['api_key']
HEADER = {"x-api-key": API_KEY}

API_URL = 'https://gazellegames.net/api.php'
REQUEST_STRING = '?request='
SEARCH_STRING = REQUEST_STRING + 'search&search_type=torrents'

SPECIAL_EDITIONS = ["redump"]
USA_REGIONS = ["USA", "NTSC"]
EUR_REGIONS = ["Europe", "PAL", "PAL-E"]
JPN_REGIONS = ["Japan", "NTSC-J"]

def generate_list(console_torrents):
    torrents_to_fetch = []
    for group, groupData in console_torrents.items():
        if 'Torrents' in groupData and groupData['Torrents']:
            for torrent, torrentData in groupData['Torrents'].items():
                if torrentData['RemasterTitle'] != "" and any(term in torrentData['RemasterTitle'].lower() for term in SPECIAL_EDITIONS):
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

        # Stop if we've reached the limit
        if len(final_torrents) >= TORRENT_LIMIT:
            final_torrents = final_torrents[:TORRENT_LIMIT]
            print(f"Reached limit of {TORRENT_LIMIT} torrents — stopping early.")
            break

        target_page += 1
        time.sleep(2.1)  # obey rate limit

    print(f"Total torrents collected: {len(final_torrents)}")
    return final_torrents

def extract_redump_nr(torrent):
    # Try HTML and BBcode descriptions
    text = torrent.get("description") or torrent.get("bbDescription") or ""
    if not text:
        return ""

    # Decode HTML entities
    text = html.unescape(text)

    # Main pattern: redump.org/disc/NUMBER
    m = re.search(r"redump\.org[\\/]+disc[\\/]+(\d+)", text)
    if m:
        return m.group(1)

    # Fallback: any "disc/NUMBER"
    m = re.search(r"disc[\\/]+(\d+)", text)
    if m:
        return m.group(1)

    return ""

def collect_torrent_metadata(torrent_ids, session):
    output_file = f"{targetConsole}_metadata.csv"

    with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'group_id', 'group_name', 'group_year',
            'torrent_id', 'info_hash', 'region', 'language',
            'remaster_year', 'remaster_title', 'release_title',
            'release_type', 'file_index', 'file_name', 'redump_nr'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

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

            # Decode HTML entities
            group_name = html.unescape(group.get('name', ''))
            release_title = html.unescape(torrent.get('releaseTitle', ''))
            remaster_title = html.unescape(torrent.get('remasterTitle', ''))

            # Extract Redump ID
            redump_nr = extract_redump_nr(torrent)

            # Handle file list (multiple files → multiple rows)
            files = torrent.get('fileList', [])
            if not files:
                writer.writerow({
                    'group_id': group.get('id'),
                    'group_name': group_name,
                    'group_year': group.get('year'),
                    'torrent_id': torrent.get('id'),
                    'info_hash': torrent.get('infoHash'),
                    'region': torrent.get('region'),
                    'language': torrent.get('language'),
                    'remaster_year': torrent.get('remasterYear'),
                    'remaster_title': remaster_title,
                    'release_title': release_title,
                    'release_type': torrent.get('releaseType'),
                    'file_index': '',
                    'file_name': '',
                    'redump_nr': redump_nr
                })
            else:
                for file_index, f in enumerate(files, start=1):
                    writer.writerow({
                        'group_id': group.get('id'),
                        'group_name': group_name,
                        'group_year': group.get('year'),
                        'torrent_id': torrent.get('id'),
                        'info_hash': torrent.get('infoHash'),
                        'region': torrent.get('region'),
                        'language': torrent.get('language'),
                        'remaster_year': torrent.get('remasterYear'),
                        'remaster_title': remaster_title,
                        'release_title': release_title,
                        'release_type': torrent.get('releaseType'),
                        'file_index': file_index,
                        'file_name': f.get('name'),
                        'redump_nr': redump_nr
                    })

            time.sleep(2.1)

    print(f"\n✅ Metadata collection complete. Saved to: {output_file}")

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

session = requests.Session()
torrent_ids = fetch_pages(session)
collect_torrent_metadata(torrent_ids, session)
