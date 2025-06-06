#!/usr/bin/env -S uv run --env-file .env

import os
import spotipy
import re
from spotipy.oauth2 import SpotifyOAuth
from spotipy.exceptions import SpotifyException
import difflib
import time
import argparse
import sys
import cache_manager # Assuming cache_manager.py is in the same directory

# --- Constants ---
DEFAULT_TRACKS_PER_RELEASE = 1 # Renamed to avoid conflict with arg name
BATCH_SIZE = 100
MAX_RETRIES = 3
RETRY_DELAY = 5
DEFAULT_DELAY = 0.1
SCOPE = 'playlist-modify-private'

# --- ANSI Color Codes ---
class Colors:
    RESET, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, BOLD = (
        "\033[0m", "\033[31m", "\033[32m", "\033[33m", "\033[34m", "\033[35m", "\033[36m", "\033[1m"
    )

def colorize(text, color):
    """Applies ANSI color codes to text."""
    return f"{color}{text}{Colors.RESET}"

# --- RateLimiter ---
class RateLimiter:
    """Limits the rate of function calls."""
    def __init__(self, delay=DEFAULT_DELAY):
        self.last_call = 0
        self.delay = delay
    def wait(self):
        """Waits if necessary to maintain the desired delay between calls."""
        elapsed = time.time() - self.last_call
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_call = time.time()
rate_limiter = RateLimiter()

# --- Initialization ---
def initialize_spotify_client():
    try:
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=os.getenv('SPOTIPY_CLIENT_ID', ''),
            client_secret=os.getenv('SPOTIPY_CLIENT_SECRET', ''),
            redirect_uri=os.getenv('SPOTIPY_REDIRECT_URI', 'http://127.0.0.1:8888/callback'),
            scope=SCOPE, open_browser=False),
            requests_timeout=20, retries=0, status_retries=0, backoff_factor=0)
        user_info = sp.current_user()
        user_id = user_info['id']
        display_name = user_info['display_name']
        print(f"Authentication {colorize('successful', Colors.GREEN)} for {colorize(display_name, Colors.CYAN)} ({colorize(user_id, Colors.CYAN)}).")
        return sp, user_id
    except Exception as e:
        print(colorize(f"Error connecting to Spotify or getting user ID: {e}", Colors.RED))
        print(colorize("Please check credentials, authorization, and network connection.", Colors.YELLOW))
        sys.exit(1)

sp, user_id = initialize_spotify_client()

# --- Helper Functions ---
def parse_line(line):
    """Parses a line from the input file into type and content parts."""
    line = line.lstrip("- ").strip()
    if ":" not in line: return None, None, None, None
    entry_type, content = map(str.strip, line.split(":", 1))
    entry_type = entry_type.lower()

    if entry_type == "title": return entry_type, content.strip('"'), None, None
    if entry_type == "url": return entry_type, None, content.strip('"'), None
    if entry_type not in ["song", "singles", "album", "ep", "compilation", "single"]: return None, None, None, None

    # Regex splits by ' - ' respecting quotes
    parts = re.split(r'\s+-\s+(?=(?:[^"]*"[^"]*")*[^"]*$)', content)
    parts = [p.strip('"') for p in parts]

    proc_type = "song" if entry_type in ["song", "singles", "single"] else "album"

    if proc_type == "song":
        if len(parts) == 2: return entry_type, parts[0], None, parts[1]   # name - artist
        if len(parts) >= 3: return entry_type, parts[0], parts[1], parts[2]  # name - album - artist
    elif proc_type == "album": # album, ep, compilation
        if len(parts) >= 2: return entry_type, parts[0], parts[1], None   # name - artist

    return None, None, None, None # Invalid format

def extract_western_name(text):
    """Extracts text within the first found brackets or parentheses."""
    for pattern in [r'\[(.*?)\]', r'\((.*?)\)']:
        match = re.search(pattern, text)
        if match: return match.group(1).strip()
    return text

def get_search_variants(name):
    """Generates search term variants (original, western, clean)."""
    if not name: return []
    variants = {name}
    western_name = extract_western_name(name)
    if western_name != name: variants.add(western_name)
    clean_name = re.sub(r'\[.*?\]|\(.*?\)', '', name).strip()
    if clean_name: variants.add(clean_name)
    return list(variants)

# --- API Call Wrapper ---
def call_with_retry(func, *args, **kwargs):
    """Calls a Spotipy function with rate limiting and retries."""
    for attempt in range(MAX_RETRIES):
        rate_limiter.wait()
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get('Retry-After', RETRY_DELAY))
                print(colorize(f"Rate limit hit. Retrying after {retry_after}s (Attempt {attempt + 1}/{MAX_RETRIES})...", Colors.YELLOW))
                time.sleep(retry_after)
            else:
                print(colorize(f"Spotify API Error ({e.http_status}): {e.msg}. Aborting call.", Colors.RED))
                raise
        except Exception as e: # Catch broader network/request errors
            print(colorize(f"Network/Other Error: {e} (Attempt {attempt + 1}/{MAX_RETRIES}). Retrying...", Colors.RED))
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(colorize(f"Failed after {MAX_RETRIES} attempts.", Colors.RED))
                raise
    # Should not be reached if loop completes without returning/raising
    raise Exception(f"API call failed definitively after {MAX_RETRIES} attempts.")

# --- Search Functions ---
def search_spotify(query_type, params, search_func):
    """Generic search wrapper checking cache first."""
    cached_result = cache_manager.check_cache(query_type, params)
    if cached_result is not None:
        source = 'CACHE'
        result = None if cached_result == cache_manager.NOT_FOUND_MARKER else cached_result
        return result, source
    try:
        result = search_func(params) # Delegate actual search logic
        cache_manager.update_cache(query_type, params, result if result else cache_manager.NOT_FOUND_MARKER)
        return result, 'API'
    except Exception as e:
        print(colorize(f"Error during {query_type} search API call: {e}", Colors.RED))
        # Cache not found on error to avoid retrying broken searches immediately
        cache_manager.update_cache(query_type, params, cache_manager.NOT_FOUND_MARKER)
        return None, 'API' # Indicate API was attempted

def _perform_album_search(params):
    """Actual API search logic for an album ID."""
    album_name, artist_name = params["album_name"], params["artist_name"]
    album_variants = get_search_variants(album_name)
    artist_variants = get_search_variants(artist_name)

    # Stage 1: Exact(ish) match query
    for album_v in album_variants:
        for artist_v in artist_variants:
            query = f'album:"{album_v}" artist:"{artist_v}"'
            results = call_with_retry(sp.search, q=query, type='album', limit=1)
            if results['albums']['items']:
                album = results['albums']['items'][0]
                found_artists = [a['name'].lower() for a in album['artists']]
                artist_v_lower = artist_v.lower()
                # Check if provided artist variant is in found artists (or vice versa)
                if any(artist_v_lower in fa or fa in artist_v_lower for fa in found_artists):
                    return album['id']

    # Stage 2: Fuzzy match query (broader album search, then filter by artist similarity)
    for album_v in album_variants:
        query = f'album:"{album_v}"'
        results = call_with_retry(sp.search, q=query, type='album', limit=5)
        for album in results['albums']['items']:
            album_artists_lower = [a['name'].lower() for a in album['artists']]
            for artist_v in artist_variants:
                artist_v_lower = artist_v.lower()
                # Check artist similarity/containment
                if any(artist_v_lower in aa or aa in artist_v_lower or difflib.SequenceMatcher(None, artist_v_lower, aa).ratio() > 0.8 for aa in album_artists_lower):
                    return album['id']
    return None

def search_album(album_name, artist_name):
    """Searches for an album, using cache first."""
    return search_spotify('search_album', {"album_name": album_name, "artist_name": artist_name}, _perform_album_search)

def _perform_song_search(params):
    """Actual API search logic for a song."""
    song_name, album_name, artist_name = params["song_name"], params.get("album_name"), params["artist_name"]
    # Use only primary variants for focused search attempts
    song_v = get_search_variants(song_name)[0] if song_name else None
    artist_v = get_search_variants(artist_name)[0] if artist_name else None
    album_v = get_search_variants(album_name)[0] if album_name else None

    if not song_v or not artist_v: return None # Essential info missing

    def check_match(track, require_album_match=False, fuzzy=False):
        """Checks if a found track matches the search criteria."""
        track_name_lower = track['name'].lower()
        track_artists_lower = [a['name'].lower() for a in track['artists']]
        song_v_lower = song_v.lower()
        artist_v_lower = artist_v.lower()

        song_sim = difflib.SequenceMatcher(None, song_v_lower, track_name_lower).ratio()
        artist_match = any(artist_v_lower in ta or ta in artist_v_lower or difflib.SequenceMatcher(None, artist_v_lower, ta).ratio() > 0.8 for ta in track_artists_lower)

        # Adjust song similarity threshold based on fuzzy flag
        song_threshold = 0.8 if fuzzy else 0.95

        if song_sim < song_threshold or not artist_match: return None

        # Check album similarity if required
        if require_album_match and album_v:
            track_album_lower = track['album']['name'].lower()
            album_v_lower = album_v.lower()
            album_sim = difflib.SequenceMatcher(None, album_v_lower, track_album_lower).ratio()
            # Adjust album similarity threshold based on fuzzy flag
            album_threshold = 0.7 if fuzzy else 0.8
            if album_sim <= album_threshold: return None # Album doesn't match well enough

        return {'id': track['id'], 'name': track['name'], 'album': track['album']['name'], 'fuzzy_matched': fuzzy}

    # --- Search Stages ---
    # Stage 1: Exact query using track, artist, and album fields
    if album_v:
        query = f'track:"{song_v}" album:"{album_v}" artist:"{artist_v}"'
        results = call_with_retry(sp.search, q=query, type='track', limit=1)
        if results['tracks']['items']:
            match = check_match(results['tracks']['items'][0], require_album_match=True)
            if match: return match

    # Stage 2: Query using track and artist fields, then check album similarity
    query = f'track:"{song_v}" artist:"{artist_v}"'
    results = call_with_retry(sp.search, q=query, type='track', limit=5)
    for track in results['tracks']['items']:
        match = check_match(track, require_album_match=True) # Check album if provided
        if match: return match

    # Stage 3: Looser query (song + artist text), stricter post-filtering (fuzzy check)
    query = f'{song_v} {artist_v}'
    results = call_with_retry(sp.search, q=query, type='track', limit=5)
    for track in results['tracks']['items']:
        match = check_match(track, require_album_match=True, fuzzy=True)
        if match: return match

    return None # Not found after all stages

def search_song(song_name, album_name, artist_name):
    """Searches for a song, using cache first."""
    params = {"song_name": song_name, "artist_name": artist_name}
    if album_name: params["album_name"] = album_name # Only include album if provided
    return search_spotify('search_song', params, _perform_song_search)

def get_album_track_details(album_id):
    """Fetches and caches full track details (id, name, popularity) for an album."""
    query_type = cache_manager.ALBUM_DETAILS_TYPE
    params = {"album_id": album_id}
    cached_details = cache_manager.check_cache(query_type, params)

    # Check cache validity
    if isinstance(cached_details, list): return cached_details, 'CACHE'
    if cached_details == cache_manager.NOT_FOUND_MARKER: return [], 'CACHE'

    # Fetch from API if not valid in cache
    source = 'API'
    full_details_list = []
    try:
        # Step 1: Get all track IDs (paginated)
        all_track_ids = []
        offset = 0
        while True:
            results = call_with_retry(sp.album_tracks, album_id, limit=50, offset=offset)
            page_tracks = results.get('items', [])
            if not page_tracks: break
            all_track_ids.extend([track['id'] for track in page_tracks if track and track.get('id')])
            offset += len(page_tracks)
            if len(page_tracks) < 50: break # Last page

        # Step 2: Get full track details (including popularity) in batches
        for i in range(0, len(all_track_ids), 50):
            batch_ids = all_track_ids[i:i+50]
            track_details_batch = call_with_retry(sp.tracks, batch_ids)
            for track_data in track_details_batch.get('tracks', []):
                if track_data and track_data.get('id'):
                    full_details_list.append({
                        'id': track_data['id'],
                        'popularity': track_data.get('popularity', 0),
                        'name': track_data.get('name', 'N/A') # Handle potentially missing names
                    })
        cache_manager.update_cache(query_type, params, full_details_list)
        return full_details_list, source
    except Exception as e:
        print(colorize(f"Error fetching track details for album {album_id}: {e}", Colors.RED))
        return [], source # Don't cache failures, return empty list

def get_top_tracks_from_album(album_id, count=1, exclude_ids=None):
    """Gets the top N tracks from an album based on popularity, excluding specified IDs."""
    full_details_list, source = get_album_track_details(album_id)
    if not full_details_list: return [], source

    exclude_ids_set = set(exclude_ids) if exclude_ids else set()
    # Filter out excluded tracks and sort by popularity (desc)
    eligible_tracks = [t for t in full_details_list if t.get('id') not in exclude_ids_set]
    sorted_tracks = sorted(eligible_tracks, key=lambda x: x.get('popularity', 0), reverse=True)
    # Select top 'count' tracks and format output
    top_tracks_output = [{'id': t['id'], 'name': t['name']} for t in sorted_tracks[:count]]
    return top_tracks_output, source

# --- Playlist Addition ---
def add_tracks_to_playlist(playlist_id, track_ids):
    """Adds a list of track IDs to a Spotify playlist in batches."""
    # Clean and deduplicate track IDs
    track_ids_clean = list(dict.fromkeys([str(tid) for tid in track_ids if tid]))
    if not track_ids_clean: return 0

    total_to_add = len(track_ids_clean)
    added_count = 0
    num_batches = (total_to_add + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  -> Adding {total_to_add} tracks in {num_batches} batch(es)... ", end="")
    success = True
    for i in range(0, total_to_add, BATCH_SIZE):
        batch = track_ids_clean[i:i+BATCH_SIZE]
        try:
            call_with_retry(sp.playlist_add_items, playlist_id, batch)
            added_count += len(batch)
        except Exception as e:
            # Print error inline with batch adding status
            print(colorize(f"\n    Error adding batch {i // BATCH_SIZE + 1}/{num_batches}: {e}", Colors.RED))
            success = False # Mark as failed but continue trying other batches

    # Print final status for the overall operation
    print(colorize("OK", Colors.GREEN) if success else f"\n  -> Partial addition: {added_count}/{total_to_add} tracks added.")
    return added_count

# --- File Reading ---
def read_music_file(filepath):
    """Reads and parses the music file, returning entries and playlist metadata."""
    entries = []
    playlist_title = "New Playlist" # Default title
    playlist_description = ""      # Default description

    if not os.path.exists(filepath):
        print(colorize(f"Error: Music file not found at '{filepath}'", Colors.RED))
        sys.exit(1)

    print(f"Reading {colorize(filepath, Colors.CYAN)}: ", end="")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

        valid_entry_count = 0
        for line in lines:
            type_name, p1, p2, p3 = parse_line(line)
            if type_name == "title": playlist_title = p1
            elif type_name == "url": playlist_description = p2
            elif type_name == "song" and p1 and p3: # name, artist required for song
                entries.append({"input_type": "song", "name": p1, "album": p2, "artist": p3})
                valid_entry_count += 1
            elif type_name in ["album", "ep", "compilation", "single"] and p1 and p2: # name, artist required for album-like
                # Store original type for potential future use, process as 'album'
                entries.append({"input_type": "album", "name": p1, "artist": p2, "original_input_type": type_name})
                valid_entry_count += 1
            # Silently ignore lines that don't parse correctly

        print(f"{colorize(str(valid_entry_count), Colors.GREEN)} valid entries found.")
        if valid_entry_count == 0:
            print(colorize("No valid music entries to process. Exiting.", Colors.YELLOW))
            sys.exit(0)
        return entries, playlist_title, playlist_description

    except Exception as e:
        print(colorize(f"\nError reading music file '{filepath}': {e}", Colors.RED))
        sys.exit(1)

def process_entry(entry, processed_track_ids_this_run, tracks_per_release_count):
    """Processes a single entry, returning details of newly added tracks."""
    track_ids_to_add = []
    status_code, source_code = '?', '?'
    details_for_print = {} # Initialize as dict; will hold error, duplicates, or added tracks list
    new_tracks_found_details = [] # Store details {'name':...} of tracks actually added

    try:
        if entry['input_type'] == 'song':
            song_result, source = search_song(entry['name'], entry.get('album'), entry['artist'])
            source_code = 'C' if source == 'CACHE' else 'A'
            if song_result:
                track_id = song_result['id']
                if track_id not in processed_track_ids_this_run:
                    status_code = 'F'
                    track_ids_to_add.append(track_id)
                    new_tracks_found_details.append({'name': song_result['name']})
                    # Store added track list in details for print
                    details_for_print['added_tracks'] = new_tracks_found_details
                else:
                    status_code = 'F' # Found, but duplicate
                    details_for_print = {'name': song_result['name'], 'is_duplicate': True}
            else:
                status_code = 'N'

        elif entry['input_type'] == 'album':
            album_id, source = search_album(entry['name'], entry['artist'])
            source_code = 'C' if source == 'CACHE' else 'A'
            if album_id:
                status_code = 'F' # Album found
                found_tracks_details, track_source = get_top_tracks_from_album(
                    album_id, tracks_per_release_count, processed_track_ids_this_run
                )
                if track_source == 'API': source_code = 'A'

                if found_tracks_details:
                    # Process all new tracks returned
                    for track_detail in found_tracks_details:
                        track_id = track_detail['id']
                        if track_id not in processed_track_ids_this_run:
                            track_ids_to_add.append(track_id)
                            new_tracks_found_details.append({'name': track_detail['name']})

                    if new_tracks_found_details:
                        # Store the full list of added tracks for printing
                        details_for_print['added_tracks'] = new_tracks_found_details
                    elif found_tracks_details: # Found tracks, but none were new
                         details_for_print = {'name': found_tracks_details[0]['name'], 'is_duplicate': True, 'all_duplicates': True}
                # else: Album found, but get_top_tracks returned nothing new
            else:
                status_code = 'N' # Album not found

    except Exception as e:
        status_code = 'E'
        details_for_print = {'error': str(e)} # Store error in details

    # Return the list of *new* track IDs and the details dict for printing
    return track_ids_to_add, status_code, source_code, details_for_print


def print_entry_result(index, total, entry, status_code, source_code, details):
    """Formats and prints the processing result for a single entry."""
    idx_width = len(str(total))
    idx_str = f"[{index:>{idx_width}}/{total}]"
    entry_name = colorize(f"'{entry['name']}'", Colors.YELLOW)
    artist_name = colorize(entry.get('artist', 'N/A'), Colors.MAGENTA)
    orig_type = f" ({entry.get('original_input_type', entry.get('input_type', ''))})"

    src_color = {'C': Colors.CYAN, 'A': Colors.BLUE}.get(source_code, Colors.RED)
    stat_color = {'F': Colors.GREEN, 'N': Colors.RED, 'E': Colors.RED}.get(status_code, Colors.RED)
    stat_text = {'F': "Found", 'N': "Not Found", 'E': "Error"}.get(status_code, "Unknown")

    src_tag = colorize(f"[{source_code}]", src_color)
    stat_tag = colorize(f"[{status_code}]", stat_color)

    print(f"{idx_str} {src_tag} {stat_tag}")
    if entry.get('input_type') in ['song', 'album']:
         print(f"    ├── {entry_name} by {artist_name}{orig_type}")
    else:
         print(f"    ├── {entry_name}{orig_type}")

    # --- Detail Printing Logic ---
    if details:
        if 'error' in details:
            print(f"    └── Error: {colorize(details['error'], Colors.RED)}")
        elif details.get('all_duplicates'):
            # Album found, but all tracks returned were already processed
            print(f"    └── Track: '{details.get('name', 'N/A')}' ({colorize('All found tracks were duplicates', Colors.YELLOW)})")
        elif details.get('is_duplicate'):
            # Song found, but already processed
             print(f"    └── Track: '{details.get('name', 'N/A')}' ({colorize('Duplicate', Colors.YELLOW)})")
        elif 'added_tracks' in details and details['added_tracks']:
            # One or more tracks were newly added for this entry
            added_tracks = details['added_tracks']
            # Print first track with the standard prefix
            print(f"    └── Added: '{added_tracks[0]['name']}'")
            # Print subsequent added tracks with indentation
            for track_detail in added_tracks[1:]:
                print(f"        Added: '{track_detail['name']}'")
        else:
            # Fallback if details dict is present but doesn't match known cases
             print(f"    └── Status: {stat_text}")

    elif status_code == 'N':
        print(f"    └── {colorize('Not found on Spotify.', Colors.YELLOW)}")
    else: # Fallback for unexpected states without details
        print(f"    └── Status: {stat_text}")

    print() # Add visual separation between entries

# --- Main Function ---
def main():
    """Main execution flow."""
    parser = argparse.ArgumentParser(description='Create a Spotify playlist from a structured TXT.')
    parser.add_argument('music_file', nargs='?', default='music.txt',
                        help='Path to the music file (default: music.txt)')
    parser.add_argument('--clear-cache', action='store_true',
                        help='Clear the API cache before running.')
    # Use the constant's value for the default argument here
    parser.add_argument('--tracks-per-release', type=int, default=DEFAULT_TRACKS_PER_RELEASE,
                        help=f'Tracks to add per album/EP/single (default: {DEFAULT_TRACKS_PER_RELEASE})')
    args = parser.parse_args()

    # Get the number of tracks per release from args (uses default if not specified)
    tracks_per_release_count = args.tracks_per_release

    if args.clear_cache:
        cache_manager.clear_cache()
        print("Cache cleared.")

    try:
        cache_manager.initialize_cache()
        print(f"Cache initialized at {colorize(os.path.abspath(cache_manager.DB_FILE), Colors.CYAN)}.")
    except Exception as e:
        print(colorize(f"FATAL: Could not initialize cache. Error: {e}", Colors.RED))
        sys.exit(1)

    entries, playlist_title, playlist_description = read_music_file(args.music_file)

    print(f"Creating playlist '{colorize(playlist_title, Colors.YELLOW)}'...", end="")
    try:
        playlist = call_with_retry(sp.user_playlist_create, user=user_id, name=playlist_title, public=False, description=playlist_description)
        playlist_id = playlist['id']
        print(f" {colorize('OK', Colors.GREEN)} (ID: {colorize(playlist_id, Colors.CYAN)})")
    except Exception as e:
        print(f" {colorize('failed', Colors.RED)}: {e}")
        sys.exit(1)

    if tracks_per_release_count != DEFAULT_TRACKS_PER_RELEASE:
         print(f"Processing with {colorize(str(tracks_per_release_count), Colors.CYAN)} tracks per album/EP/single entry.")

    track_ids_to_add_batch = []
    processed_track_ids_this_run = set()
    total_added_count_overall = 0
    total_entries = len(entries)

    print("\n--- Processing Entries ---\n")
    for i, entry in enumerate(entries):
        # Pass the count derived from args down to process_entry
        new_track_ids, status, source, details = process_entry(
            entry, processed_track_ids_this_run, tracks_per_release_count
        )

        print_entry_result(i + 1, total_entries, entry, status, source, details)

        if new_track_ids:
            track_ids_to_add_batch.extend(new_track_ids)
            processed_track_ids_this_run.update(new_track_ids)

        if len(track_ids_to_add_batch) >= BATCH_SIZE:
            print(f"  -> Reached batch size ({BATCH_SIZE}). Adding tracks...")
            added_this_batch = add_tracks_to_playlist(playlist_id, track_ids_to_add_batch)
            total_added_count_overall += added_this_batch
            track_ids_to_add_batch = []
            print()

    if track_ids_to_add_batch:
        print(f"  -> Adding final batch of {len(track_ids_to_add_batch)} tracks...")
        added_this_batch = add_tracks_to_playlist(playlist_id, track_ids_to_add_batch)
        total_added_count_overall += added_this_batch
        print()

    print("--- Summary ---")
    playlist_url = f"https://open.spotify.com/playlist/{playlist_id}" # Correct URL format
    print(f"Playlist '{colorize(playlist_title, Colors.YELLOW)}' URL: {colorize(playlist_url, Colors.CYAN)}")
    print(f"{colorize(str(total_entries), Colors.GREEN)} entries processed.")
    print(f"{colorize(str(total_added_count_overall), Colors.GREEN)} unique tracks added to the playlist in this run.")

if __name__ == "__main__":
    main()