#!/usr/bin/env -S uv run --env-file .env
# /// script
# dependencies = [
#     "spotipy",
# ]
# ///

import os
import spotipy
import re
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException
import difflib
import time
import argparse
import sys

# Import the cache manager functions
import cache_manager # Assuming cache_manager.py is in the same directory

# --- Constants ---
TRACKS_PER_RELEASE = 1 # Logic remains: only 1 track added per entry
BATCH_SIZE = 100
MAX_RETRIES = 3
RETRY_DELAY = 5
DEFAULT_DELAY = 0.1

# --- Spotify Credentials ---
SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID', '')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET', '')
SPOTIPY_REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI', 'http://127.0.0.1:8888/callback')
SCOPE = 'playlist-modify-private'

# --- ANSI Color Codes ---
class Colors:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    BOLD = "\033[1m"

def colorize(text, color):
    return f"{color}{text}{Colors.RESET}"

# --- RateLimiter Class --- (Unchanged)
class RateLimiter:
    def __init__(self):
        self.last_call = 0
        self.delay = DEFAULT_DELAY
    def wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_call = time.time()
rate_limiter = RateLimiter()

# --- Initialize Spotipy Client --- (Unchanged from previous version)
sp = None
user_id = None
try:
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET,
        redirect_uri=SPOTIPY_REDIRECT_URI, scope=SCOPE, open_browser=False),
        requests_timeout=20, retries=0, status_retries=0, backoff_factor=0)
    user_info = sp.current_user()
    user_id = user_info['id']
    display_name = user_info['display_name']
    print(f"Authentication {colorize('successful', Colors.GREEN)} for {colorize(display_name, Colors.CYAN)} ({colorize(user_id, Colors.CYAN)}).")
except Exception as e:
    print(colorize(f"Error connecting to Spotify or getting user ID: {e}", Colors.RED))
    print(colorize("Please check credentials, authorization, and network connection.", Colors.YELLOW))
    sys.exit(1)

# --- Helper Functions --- (Unchanged)
def debug_line(line, album_title, artist_name): print(f"DEBUG: Line='{line}'\n  -> Parsed Album: '{album_title}'\n  -> Parsed Artist: '{artist_name}'")

def parse_line(line):
    # ... (parsing logic unchanged) ...
    line = line.lstrip("- ").strip()
    if ":" not in line:
        return None, None, None, None

    parts = line.split(":", 1)
    entry_type = parts[0].strip().lower()
    content = parts[1].strip()

    if entry_type in ["title", "url"]:
        if content.startswith('"') and content.endswith('"'):
            content = content[1:-1]
        return entry_type, content, None, None

    if entry_type in ["song", "singles", "album", "ep", "compilation", "single"]:
        content_parts = []
        current = ""
        in_quotes = False
        i = 0
        while i < len(content):
            char = content[i]
            if char == '"':
                in_quotes = not in_quotes
                current += char
                i += 1
                continue
            if not in_quotes and content[i:i+3] == " - ":
                content_parts.append(current.strip())
                current = ""
                i += 3
                continue
            current += char
            i += 1
        if current:
            content_parts.append(current.strip())

        content_parts = [part.strip('"') for part in content_parts]

        if entry_type in ["song", "single"]:
            if len(content_parts) == 2:
                return "song", content_parts[0], None, content_parts[1]
            elif len(content_parts) >= 3:
                return "song", content_parts[0], content_parts[1], content_parts[2]
            else:
                return None, None, None, None

        elif entry_type in ["album", "ep", "compilation"] and len(content_parts) >= 2:
            # Map specific types like 'ep' back to 'album' for processing consistency if needed,
            # but keeping original type might be useful elsewhere. Let's return the specific type for now.
            return entry_type, content_parts[0], content_parts[1], None

    return None, None, None, None


def extract_western_name(text):
    # ... (unchanged) ...
    bracket_match = re.search(r'\[(.*?)\]', text);
    if bracket_match: return bracket_match.group(1).strip()
    paren_match = re.search(r'\((.*?)\)', text);
    if paren_match: return paren_match.group(1).strip()
    return text

def get_search_variants(name):
    # ... (unchanged) ...
    if not name: return []
    variants = [name]
    western_name = extract_western_name(name)
    if western_name != name: variants.append(western_name)
    clean_name = re.sub(r'\[.*?\]|\(.*?\)', '', name).strip()
    if clean_name and clean_name not in variants: variants.append(clean_name)
    return list(dict.fromkeys(variants))

# --- API Call Wrapper --- (Unchanged from previous version)
def call_with_retry(func, *args, **kwargs):
    # ... (unchanged) ...
    current_retries = MAX_RETRIES
    for attempt in range(current_retries):
        rate_limiter.wait()
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get('Retry-After', RETRY_DELAY))
                if attempt == current_retries - 1:
                     print(colorize(f"API rate limited. Failed after {current_retries} attempts.", Colors.YELLOW))
                time.sleep(retry_after)
                continue
            else:
                print(colorize(f"Spotify API Error ({e.http_status}): {e.msg}.", Colors.RED))
                raise
        except Exception as e:
            if attempt < current_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            else:
                print(colorize(f"Network/Other Error: {e}. Failed after {current_retries} attempts.", Colors.RED))
                raise
    raise Exception(f"API call failed after {current_retries} attempts.")


# --- Search Functions --- (Unchanged - logic and return values)
# Return format: (result, source) tuple

def search_album(album_name, artist_name):
    # ... (logic unchanged, no internal prints) ...
    query_type = 'search_album'; params = {"album_name": album_name, "artist_name": artist_name}
    cached_result = cache_manager.check_cache(query_type, params)
    source = 'CACHE'
    if cached_result is not None:
        if cached_result == cache_manager.NOT_FOUND_MARKER: return (None, source)
        else: return (cached_result, source) # album_id

    source = 'API'
    album_id = None; album_variants = get_search_variants(album_name); artist_variants = get_search_variants(artist_name); found = False
    # Try exact first
    for album_v in album_variants:
        if found: break
        for artist_v in artist_variants:
            try:
                query = f'album:"{album_v}" artist:"{artist_v}"'; results = call_with_retry(sp.search, q=query, type='album', limit=1)
                if results['albums']['items']:
                    album = results['albums']['items'][0]; found_artists = [a['name'] for a in album['artists']]
                    if any(artist_v.lower() in fa.lower() or fa.lower() in artist_v.lower() for fa in found_artists):
                        album_id = album['id']; found = True; break
            except Exception: pass
        if found: break
    # Try fuzzy if not found
    if not found:
        for album_v in album_variants:
            if found: break
            try:
                query = f'album:"{album_v}"'; results = call_with_retry(sp.search, q=query, type='album', limit=5)
                for album in results['albums']['items']:
                    album_artists_lower = [a['name'].lower() for a in album['artists']];
                    for artist_v in artist_variants:
                        artist_v_lower = artist_v.lower()
                        if any(artist_v_lower in aa_lower or aa_lower in artist_v_lower or difflib.SequenceMatcher(None, artist_v_lower, aa_lower).ratio() > 0.8 for aa_lower in album_artists_lower):
                            album_id = album['id']; found = True; break
                    if found: break
            except Exception: pass
            if found: break

    result_to_cache = album_id if album_id else cache_manager.NOT_FOUND_MARKER; cache_manager.update_cache(query_type, params, result_to_cache)
    return (album_id, source)

# Return format: (track_info, source) or (None, source)
def search_song(song_name, album_name, artist_name):
    # ... (logic unchanged, no internal prints) ...
    query_type = 'search_song'
    params = {"song_name": song_name, "album_name": album_name, "artist_name": artist_name}
    cached_result = cache_manager.check_cache(query_type, params)
    source = 'CACHE'
    if cached_result is not None:
        if cached_result == cache_manager.NOT_FOUND_MARKER: return (None, source)
        else: return (cached_result, source) # track_info dict

    source = 'API'
    track_info = None
    song_variants = get_search_variants(song_name)[:2]
    artist_variants = get_search_variants(artist_name)[:1]
    album_variants = get_search_variants(album_name)[:1] if album_name else [None]
    found = False
    fuzzy_match_occurred = False
    found_track_details = {}

    # Stage 1: Exact with album field
    for song_v in song_variants:
        if found: break
        album_v_query = album_variants[0] if album_name else None
        for artist_v in artist_variants:
            if found: break
            try:
                if album_v_query:
                    query = f'track:"{song_v}" album:"{album_v_query}" artist:"{artist_v}"'
                    results = call_with_retry(sp.search, q=query, type='track', limit=1)
                    if results['tracks']['items']:
                        track = results['tracks']['items'][0]
                        found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                        found = True; break
            except Exception: pass
    # Stage 2: Track/Artist field + Album Similarity
    if not found:
        for song_v in song_variants:
            if found: break
            for artist_v in artist_variants:
                if found: break
                try:
                    query = f'track:"{song_v}" artist:"{artist_v}"'
                    results = call_with_retry(sp.search, q=query, type='track', limit=5)
                    if results['tracks']['items']:
                        for track in results['tracks']['items']:
                            track_album_name_lower = track['album']['name'].lower()
                            if album_name:
                                album_v_to_check = album_variants[0]
                                if album_v_to_check:
                                    album_v_check_lower = album_v_to_check.lower()
                                    album_similarity = difflib.SequenceMatcher(None, album_v_check_lower, track_album_name_lower).ratio()
                                    if album_similarity > 0.8:
                                        found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                                        found = True; break
                            else:
                                found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                                found = True; break
                except Exception: pass
                if found: break
            if found: break
    # Stage 3: Fuzzy
    if not found:
        for song_v in song_variants:
            if found: break
            for artist_v in artist_variants:
                if found: break
                try:
                    query = f'{song_v} {artist_v}'
                    results = call_with_retry(sp.search, q=query, type='track', limit=5)
                    for track in results['tracks']['items']:
                        track_name_lower = track['name'].lower()
                        track_artists_lower = [a['name'].lower() for a in track['artists']]
                        song_v_lower = song_v.lower()
                        artist_v_lower = artist_v.lower()
                        song_similarity = difflib.SequenceMatcher(None, song_v_lower, track_name_lower).ratio()
                        artist_match = any(artist_v_lower in ta or ta in artist_v_lower or difflib.SequenceMatcher(None, artist_v_lower, ta).ratio() > 0.8 for ta in track_artists_lower)

                        if song_similarity >= 0.8 and artist_match:
                            album_match_ok = False
                            if album_name:
                                album_v_to_check = album_variants[0]
                                if album_v_to_check:
                                    track_album_lower = track['album']['name'].lower()
                                    album_provided_lower = album_v_to_check.lower()
                                    album_similarity = difflib.SequenceMatcher(None, album_provided_lower, track_album_lower).ratio()
                                    if album_similarity > 0.7: album_match_ok = True
                                else: album_match_ok = True
                            else: album_match_ok = True

                            if album_match_ok:
                                fuzzy_match_occurred = True
                                found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                                found = True; break
                except Exception: pass
                if found: break
            if found: break

    # Result handling
    if found:
        track_info = {
            'id': found_track_details['id'],
            'name': found_track_details['name'],
            'album': found_track_details['album'],
            'fuzzy_matched': fuzzy_match_occurred
        }
        cache_manager.update_cache(query_type, params, track_info)
        return (track_info, source)
    else:
        cache_manager.update_cache(query_type, params, cache_manager.NOT_FOUND_MARKER)
        return (None, source)


# Return format: (track_list, source)
def get_top_tracks_from_album(album_id, count=1, exclude_ids=None):
    # ... (logic unchanged, no internal prints) ...
    query_type = cache_manager.ALBUM_DETAILS_TYPE; params = {"album_id": album_id}
    cached_full_details = cache_manager.check_cache(query_type, params)
    source = 'CACHE'
    full_details_list = None

    if cached_full_details is not None:
        if isinstance(cached_full_details, list) and cached_full_details and isinstance(cached_full_details[0], dict) and 'id' in cached_full_details[0] and 'name' in cached_full_details[0]:
             full_details_list = cached_full_details
        else:
             cache_manager.clear_specific_cache(query_type, params)
             cached_full_details = None
             source = 'API'

    if full_details_list is None:
        source = 'API'
        fetched_successfully = False
        try:
            all_track_ids = []; offset = 0; limit = 50
            while True:
                results = call_with_retry(sp.album_tracks, album_id, limit=limit, offset=offset); page_tracks = results.get('items', [])
                if not page_tracks: break
                for track in page_tracks:
                    if track and track.get('id'): all_track_ids.append(track['id'])
                offset += len(page_tracks);
                if len(page_tracks) < limit: break

            temp_details_list = []
            if all_track_ids:
                for i in range(0, len(all_track_ids), 50):
                    batch_ids = all_track_ids[i:i+50]
                    try:
                        track_details_batch = call_with_retry(sp.tracks, batch_ids)
                        for track_data in track_details_batch['tracks']:
                            if track_data and track_data.get('id'):
                                temp_details_list.append({
                                    'id': track_data['id'],
                                    'popularity': track_data.get('popularity', 0),
                                    'name': track_data.get('name', 'N/A')
                                })
                    except Exception: pass

            full_details_list = temp_details_list; fetched_successfully = True
        except Exception: full_details_list = []

        if fetched_successfully and full_details_list is not None:
             cache_manager.update_cache(query_type, params, full_details_list)

    if not full_details_list: return ([], source)

    effective_exclude_ids = set(exclude_ids) if exclude_ids else set()
    eligible_tracks = [track for track in full_details_list if track.get('id') and track['id'] not in effective_exclude_ids]
    sorted_tracks = sorted(eligible_tracks, key=lambda x: x.get('popularity', 0), reverse=True)
    top_tracks_details = sorted_tracks[:count]
    top_tracks_output = [{'id': t['id'], 'name': t['name']} for t in top_tracks_details if t.get('id')]

    return (top_tracks_output, source)


# --- Playlist Addition Function --- (Unchanged from previous version)
def add_tracks_to_playlist(playlist_id, track_ids):
    # ... (logic unchanged, minimal/no prints) ...
    if not track_ids: return 0
    track_ids_clean = [str(tid) for tid in track_ids if tid]
    if not track_ids_clean: return 0
    unique_track_ids_ordered = list(dict.fromkeys(track_ids_clean))
    total_to_add = len(unique_track_ids_ordered)
    num_batches = (total_to_add + BATCH_SIZE - 1) // BATCH_SIZE
    added_count = 0

    for i in range(0, total_to_add, BATCH_SIZE):
        batch = unique_track_ids_ordered[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        try:
            call_with_retry(sp.playlist_add_items, playlist_id, batch)
            added_count += len(batch)
        except Exception as e:
             print(colorize(f"Error adding batch {batch_num}/{num_batches} to playlist {playlist_id}: {e}", Colors.RED))

    return added_count

# --- Main Function --- (Output formatting updated)
def main():
    parser = argparse.ArgumentParser(description='Create a Spotify playlist from a structured TXT (use rym-to-txt.py).')
    parser.add_argument('music_file', nargs='?', default='music.txt', help='Path to the music file (default: music.txt)')
    parser.add_argument('--clear-cache', action='store_true', help='Clear the API cache before running.')
    args = parser.parse_args()
    if args.clear_cache: cache_manager.clear_cache()

    try:
        cache_manager.initialize_cache()
        cache_path = os.path.abspath(cache_manager.DB_FILE)
        print(f"Cache initialized at {colorize(cache_path, Colors.CYAN)}.")
    except Exception as e: print(colorize(f"FATAL: Could not initialize cache. Error: {e}", Colors.RED)); sys.exit(1)

    entries = []; playlist_title = "New Playlist"; playlist_description = ""; music_file_path = args.music_file
    try:
        print(f"Reading {colorize(music_file_path, Colors.CYAN)}: ", end="")
        valid_entry_count = 0
        with open(music_file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith('#'): continue
                # Use descriptive keys for clarity in the entries list
                type_name, p1, p2, p3 = parse_line(line)
                if type_name == "title": playlist_title = p1
                elif type_name == "url": playlist_description = p2 # Corrected variable use
                elif type_name == "song":
                     if p1 and p3: # Need at least name and artist
                         entries.append({"input_type": "song", "name": p1, "album": p2, "artist": p3})
                         valid_entry_count += 1
                elif type_name in ["album", "ep", "compilation"]:
                     if p1 and p2: # Need name and artist
                        # Use 'album' as the processing type, store original if needed
                        entries.append({"input_type": "album", "name": p1, "album": None, "artist": p2, "original_input_type": type_name})
                        valid_entry_count += 1

        print(f"{colorize(str(valid_entry_count), Colors.GREEN)} valid entries found.")
    except FileNotFoundError: print(colorize(f"\nError: Music file not found at '{music_file_path}'", Colors.RED)); sys.exit(1)
    except Exception as e: print(colorize(f"\nError reading music file '{music_file_path}': {e}", Colors.RED)); sys.exit(1)
    if not entries: print(colorize("No valid music entries found. Exiting.", Colors.YELLOW)); sys.exit(0)

    print(f"Playlist '{colorize(playlist_title, Colors.YELLOW)}' ", end="")
    playlist_id = None
    try:
        playlist = call_with_retry(sp.user_playlist_create, user=user_id, name=playlist_title, public=False, description=playlist_description)
        playlist_id = playlist['id']; print(f"created: {colorize('OK', Colors.GREEN)} (ID: {colorize(playlist_id, Colors.CYAN)})")
    except Exception as e: print(f"{colorize('creation failed', Colors.RED)}: {e}"); sys.exit(1)

    track_ids_to_add_batch = []
    processed_track_ids_this_run = set()
    total_added_count_overall = 0
    total_entries = len(entries)
    max_index_width = len(str(total_entries))

    print("\n--- Processing Entries ---")
    # Add a single newline before the first entry for spacing
    print()

    for i, entry in enumerate(entries):
        index_str = f"[{i+1:>{max_index_width}}/{total_entries}]"
        # Use entry['name'] for the display name, regardless of input type
        entry_name_colored = colorize(f"'{entry['name']}'", Colors.YELLOW)
        artist_name_colored = colorize(entry['artist'], Colors.MAGENTA)

        source_tag_char = '?'
        status_tag_char = '?'
        source_color = Colors.RED
        status_color = Colors.RED
        details_line = None # Will hold track or error info
        source = 'N/A'

        try:
            if entry['input_type'] == 'song':
                song_name = entry['name']; album_name = entry['album']; artist_name = entry['artist']
                song_result, source = search_song(song_name, album_name, artist_name)

                source_tag_char = 'C' if source == 'CACHE' else 'A'
                source_color = Colors.CYAN if source == 'CACHE' else Colors.BLUE

                if song_result:
                    track_id = song_result['id']
                    is_duplicate = track_id in processed_track_ids_this_run
                    status_tag_char = 'F'
                    status_color = Colors.GREEN

                    if not is_duplicate:
                        track_name = song_result['name']
                        # Track name uses default terminal color (often white)
                        details_line = f"    └── Track: '{track_name}'"
                        track_ids_to_add_batch.append(track_id)
                        processed_track_ids_this_run.add(track_id)
                    # else: Duplicate -> Status 'F', no details_line

                else: # song_result is None
                    status_tag_char = 'N'
                    status_color = Colors.RED

            elif entry['input_type'] == 'album':
                album_name = entry['name']; artist_name = entry['artist']
                album_id, source = search_album(album_name, artist_name)
                source_tag_char = 'C' if source == 'CACHE' else 'A'
                source_color = Colors.CYAN if source == 'CACHE' else Colors.BLUE

                if album_id:
                    found_tracks_details, track_source = get_top_tracks_from_album(album_id, TRACKS_PER_RELEASE, processed_track_ids_this_run)

                    if track_source == 'API': # Prioritize API source tag if track fetch hit API
                         source_tag_char = 'A'; source_color = Colors.BLUE

                    status_tag_char = 'F'
                    status_color = Colors.GREEN

                    if found_tracks_details:
                         track_detail = found_tracks_details[0]
                         track_id = track_detail['id']
                         if track_id not in processed_track_ids_this_run: # Check again (belt-and-suspenders)
                             track_name = track_detail['name']
                             # Track name uses default terminal color
                             details_line = f"    └── Track: '{track_name}'"
                             track_ids_to_add_batch.append(track_id)
                             processed_track_ids_this_run.add(track_id)
                         # else: Duplicate -> Status 'F', no details_line
                    # else: No new track found -> Status 'F', no details_line

                else: # album_id is None
                    status_tag_char = 'N'
                    status_color = Colors.RED

        except Exception as e:
            status_tag_char = 'E'
            status_color = Colors.RED
            if source == 'N/A': source_tag_char = '?'; source_color = Colors.RED
            details_line = f"    └── Error: {colorize(str(e), Colors.RED)}"


        # --- Print the results for this entry ---
        source_tag_colored = colorize(f"[{source_tag_char}]", source_color)
        status_tag_colored = colorize(f"[{status_tag_char}]", status_color)

        # Line 1: Status
        print(f"{index_str} {source_tag_colored} {status_tag_colored}")
        # Line 2: Entry Info
        print(f"    ├── {entry_name_colored} by {artist_name_colored}")

        # Line 3: Track or Error (if applicable)
        if details_line:
            print(details_line)

        # Single blank line between entries
        print()

        # --- Batch adding ---
        if len(track_ids_to_add_batch) >= BATCH_SIZE:
            added_this_batch = add_tracks_to_playlist(playlist_id, track_ids_to_add_batch)
            total_added_count_overall += added_this_batch
            track_ids_to_add_batch = []


    # --- Add final batch ---
    if track_ids_to_add_batch:
        added_this_batch = add_tracks_to_playlist(playlist_id, track_ids_to_add_batch)
        total_added_count_overall += added_this_batch

    # --- Final Summary ---
    print(f"\n--- Summary ---")
    print(f"Playlist '{colorize(playlist_title, Colors.YELLOW)}' ({colorize(playlist_id, Colors.CYAN)}) ready.")
    print(f"{colorize(str(total_entries), Colors.GREEN)} entries processed, {colorize(str(total_added_count_overall), Colors.GREEN)} unique tracks added this run.")

if __name__ == "__main__":
    main()