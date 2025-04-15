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

# --- Initialize Spotipy Client --- (Refactored for early exit on error)
def initialize_spotify_client():
    """Initializes the Spotipy client and returns the client object and user ID."""
    try:
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET,
            redirect_uri=SPOTIPY_REDIRECT_URI, scope=SCOPE, open_browser=False),
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

# --- Helper Functions --- (Largely unchanged, minor tweaks in parse_line)
def debug_line(line, album_title, artist_name): print(f"DEBUG: Line='{line}'\n  -> Parsed Album: '{album_title}'\n  -> Parsed Artist: '{artist_name}'")

def parse_line(line):
    """Parses a line from the input file."""
    line = line.lstrip("- ").strip()
    if ":" not in line:
        return None, None, None, None # Guard clause

    parts = line.split(":", 1)
    entry_type = parts[0].strip().lower()
    content = parts[1].strip()

    if entry_type in ["title", "url"]:
        if content.startswith('"') and content.endswith('"'):
            content = content[1:-1]
        # Return p2 as None for title, p1 as None for url for consistency
        return (entry_type, content, None, None) if entry_type == "title" else (entry_type, None, content, None)


    if entry_type not in ["song", "singles", "album", "ep", "compilation", "single"]:
        return None, None, None, None # Guard clause

    # --- Content Parsing Logic (unchanged complexity due to quotes) ---
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
    # --- End Content Parsing ---

    # Use simplified types for processing where applicable
    proc_type = "song" if entry_type in ["song", "singles", "single"] else "album"

    if proc_type == "song":
        if len(content_parts) == 2: # name - artist
            return entry_type, content_parts[0], None, content_parts[1]
        elif len(content_parts) >= 3: # name - album - artist
             return entry_type, content_parts[0], content_parts[1], content_parts[2]
        else:
             return None, None, None, None # Invalid song format

    if proc_type == "album": # album, ep, compilation
        if len(content_parts) >= 2: # name - artist
            # Keep original type if needed later, return consistent structure
            return entry_type, content_parts[0], content_parts[1], None
        else:
            return None, None, None, None # Invalid album format

    return None, None, None, None # Should not be reached, but safe fallback


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
    return list(dict.fromkeys(variants)) # Efficient unique preservation

# --- API Call Wrapper --- (Refactored with early return on success)
def call_with_retry(func, *args, **kwargs):
    """Calls a function with retries on specific Spotify exceptions."""
    for attempt in range(MAX_RETRIES):
        rate_limiter.wait()
        try:
            result = func(*args, **kwargs)
            return result # Return immediately on success
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get('Retry-After', RETRY_DELAY))
                print(colorize(f"Rate limit hit. Retrying after {retry_after}s (Attempt {attempt + 1}/{MAX_RETRIES})...", Colors.YELLOW))
                time.sleep(retry_after)
                # Continue to next attempt
            else:
                print(colorize(f"Spotify API Error ({e.http_status}): {e.msg}. Aborting call.", Colors.RED))
                raise # Re-raise non-retryable Spotify errors
        except Exception as e:
            print(colorize(f"Network/Other Error: {e} (Attempt {attempt + 1}/{MAX_RETRIES}). Retrying...", Colors.RED))
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1)) # Exponential backoff for general errors
                # Continue to next attempt
            else:
                print(colorize(f"Failed after {MAX_RETRIES} attempts.", Colors.RED))
                raise # Re-raise after final attempt fails
    # If loop completes without returning or raising, something unexpected happened
    raise Exception(f"API call failed definitively after {MAX_RETRIES} attempts.")


# --- Search Functions --- (Refactored with early returns)
# Return format: (result, source) tuple

def search_album(album_name, artist_name):
    """Searches for an album ID on Spotify, using cache first."""
    query_type = 'search_album'; params = {"album_name": album_name, "artist_name": artist_name}
    cached_result = cache_manager.check_cache(query_type, params)

    if cached_result is not None:
        source = 'CACHE'
        return (None, source) if cached_result == cache_manager.NOT_FOUND_MARKER else (cached_result, source)

    source = 'API'
    album_variants = get_search_variants(album_name); artist_variants = get_search_variants(artist_name)

    # Stage 1: Exact match attempts
    for album_v in album_variants:
        for artist_v in artist_variants:
            try:
                query = f'album:"{album_v}" artist:"{artist_v}"'; results = call_with_retry(sp.search, q=query, type='album', limit=1)
                if results['albums']['items']:
                    album = results['albums']['items'][0]; found_artists = [a['name'] for a in album['artists']]
                    # Check if *any* variant of the provided artist matches *any* returned artist loosely
                    if any(artist_v.lower() in fa.lower() or fa.lower() in artist_v.lower() for fa in found_artists):
                        album_id = album['id']
                        cache_manager.update_cache(query_type, params, album_id)
                        return (album_id, source) # Early return on exact match
            except Exception: pass # Ignore errors during search attempts, try next variant

    # Stage 2: Fuzzy match attempts
    for album_v in album_variants:
        try:
            query = f'album:"{album_v}"'; results = call_with_retry(sp.search, q=query, type='album', limit=5)
            for album in results['albums']['items']:
                album_artists_lower = [a['name'].lower() for a in album['artists']]
                for artist_v in artist_variants:
                    artist_v_lower = artist_v.lower()
                    # Check similarity or containment
                    if any(artist_v_lower in aa_lower or aa_lower in artist_v_lower or difflib.SequenceMatcher(None, artist_v_lower, aa_lower).ratio() > 0.8 for aa_lower in album_artists_lower):
                        album_id = album['id']
                        cache_manager.update_cache(query_type, params, album_id)
                        return (album_id, source) # Early return on fuzzy match
        except Exception: pass # Ignore errors

    # If no match found after all attempts
    cache_manager.update_cache(query_type, params, cache_manager.NOT_FOUND_MARKER)
    return (None, source)

def search_song(song_name, album_name, artist_name):
    """Searches for a song on Spotify, using cache first and multiple search strategies."""
    query_type = 'search_song'
    params = {"song_name": song_name, "album_name": album_name, "artist_name": artist_name}
    cached_result = cache_manager.check_cache(query_type, params)

    if cached_result is not None:
        source = 'CACHE'
        return (None, source) if cached_result == cache_manager.NOT_FOUND_MARKER else (cached_result, source)

    source = 'API'
    song_variants = get_search_variants(song_name)[:2]  # Limit variants
    artist_variants = get_search_variants(artist_name)[:1]
    album_variants = get_search_variants(album_name)[:1] if album_name else [None]
    found_track_details = None
    fuzzy_match_occurred = False

    # Prepare basic variants for loops
    primary_song_v = song_variants[0] if song_variants else None
    primary_artist_v = artist_variants[0] if artist_variants else None
    primary_album_v = album_variants[0] # Can be None

    if not primary_song_v or not primary_artist_v:
         cache_manager.update_cache(query_type, params, cache_manager.NOT_FOUND_MARKER)
         return (None, source) # Cannot search without song and artist

    # Stage 1: Exact with album field (if album provided)
    if primary_album_v:
        try:
            query = f'track:"{primary_song_v}" album:"{primary_album_v}" artist:"{primary_artist_v}"'
            results = call_with_retry(sp.search, q=query, type='track', limit=1)
            if results['tracks']['items']:
                track = results['tracks']['items'][0]
                found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                # Go directly to result handling
        except Exception: pass

    # Stage 2: Track/Artist field + Album Similarity (if not found in stage 1)
    if not found_track_details:
        try:
            query = f'track:"{primary_song_v}" artist:"{primary_artist_v}"'
            results = call_with_retry(sp.search, q=query, type='track', limit=5) # Wider search
            if results['tracks']['items']:
                for track in results['tracks']['items']:
                    # If album was provided, check similarity
                    if primary_album_v:
                        track_album_name_lower = track['album']['name'].lower()
                        album_v_check_lower = primary_album_v.lower()
                        album_similarity = difflib.SequenceMatcher(None, album_v_check_lower, track_album_name_lower).ratio()
                        if album_similarity > 0.8:
                            found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                            break # Found suitable track
                    else:
                        # No album provided, first result is good enough here
                        found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                        break # Found suitable track
        except Exception: pass

    # Stage 3: Fuzzy (if still not found) - Looser query, stricter post-filtering
    if not found_track_details:
         try:
            query = f'{primary_song_v} {primary_artist_v}' # General query
            results = call_with_retry(sp.search, q=query, type='track', limit=5)
            for track in results['tracks']['items']:
                track_name_lower = track['name'].lower()
                track_artists_lower = [a['name'].lower() for a in track['artists']]
                song_v_lower = primary_song_v.lower()
                artist_v_lower = primary_artist_v.lower()

                song_similarity = difflib.SequenceMatcher(None, song_v_lower, track_name_lower).ratio()
                artist_match = any(artist_v_lower in ta or ta in artist_v_lower or difflib.SequenceMatcher(None, artist_v_lower, ta).ratio() > 0.8 for ta in track_artists_lower)

                if song_similarity >= 0.8 and artist_match:
                    album_match_ok = True # Assume ok if no album provided
                    if primary_album_v:
                        track_album_lower = track['album']['name'].lower()
                        album_provided_lower = primary_album_v.lower()
                        album_similarity = difflib.SequenceMatcher(None, album_provided_lower, track_album_lower).ratio()
                        if album_similarity <= 0.7: # Only fail if album provided AND similarity is low
                            album_match_ok = False

                    if album_match_ok:
                        fuzzy_match_occurred = True
                        found_track_details = {'id': track['id'], 'name': track['name'], 'album': track['album']['name']}
                        break # Found suitable fuzzy match
         except Exception: pass

    # --- Result Handling ---
    if found_track_details:
        track_info = {
            'id': found_track_details['id'],
            'name': found_track_details['name'],
            'album': found_track_details['album'],
            'fuzzy_matched': fuzzy_match_occurred # Keep track if fuzzy logic was used
        }
        cache_manager.update_cache(query_type, params, track_info)
        return (track_info, source)
    else:
        # Not found after all stages
        cache_manager.update_cache(query_type, params, cache_manager.NOT_FOUND_MARKER)
        return (None, source)


def get_album_track_details(album_id):
    """Fetches and caches full track details (id, name, popularity) for an album."""
    query_type = cache_manager.ALBUM_DETAILS_TYPE; params = {"album_id": album_id}
    cached_full_details = cache_manager.check_cache(query_type, params)

    # Validate cache structure slightly more robustly
    if isinstance(cached_full_details, list):
        if not cached_full_details or (isinstance(cached_full_details[0], dict) and 'id' in cached_full_details[0]):
             # Cache is valid (list of dicts with id, or empty list)
            return cached_full_details, 'CACHE'
        else:
            # Invalid cache structure, clear it and fetch fresh
            cache_manager.clear_specific_cache(query_type, params)

    # --- Fetch from API ---
    source = 'API'
    all_track_ids = []
    full_details_list = []
    try:
        # Step 1: Get all track IDs from the album (paginated)
        offset = 0; limit = 50
        while True:
            results = call_with_retry(sp.album_tracks, album_id, limit=limit, offset=offset)
            page_tracks = results.get('items', [])
            if not page_tracks: break
            all_track_ids.extend([track['id'] for track in page_tracks if track and track.get('id')])
            offset += len(page_tracks)
            if len(page_tracks) < limit: break # Last page

        # Step 2: Get full track details (including popularity) in batches
        if all_track_ids:
            for i in range(0, len(all_track_ids), 50):
                batch_ids = all_track_ids[i:i+50]
                track_details_batch = call_with_retry(sp.tracks, batch_ids)
                for track_data in track_details_batch['tracks']:
                    if track_data and track_data.get('id'):
                        full_details_list.append({
                            'id': track_data['id'],
                            'popularity': track_data.get('popularity', 0),
                            'name': track_data.get('name', 'N/A')
                        })
        # Cache the successfully fetched list (even if empty)
        cache_manager.update_cache(query_type, params, full_details_list)
        return full_details_list, source

    except Exception as e:
        print(colorize(f"Error fetching track details for album {album_id}: {e}", Colors.RED))
        # Don't cache failures, return empty list indicating failure
        return [], source


def get_top_tracks_from_album(album_id, count=1, exclude_ids=None):
    """Gets the top N tracks from an album based on popularity, excluding specified IDs."""
    full_details_list, source = get_album_track_details(album_id)

    if not full_details_list:
        return [], source # Return empty list if fetch failed or album has no tracks

    effective_exclude_ids = set(exclude_ids) if exclude_ids else set()

    # Filter, sort, and select top tracks
    eligible_tracks = [
        track for track in full_details_list
        if track.get('id') and track['id'] not in effective_exclude_ids
    ]
    # Sort by popularity (descending)
    sorted_tracks = sorted(eligible_tracks, key=lambda x: x.get('popularity', 0), reverse=True)

    # Select the top 'count' tracks and format output
    top_tracks_output = [
        {'id': t['id'], 'name': t['name']}
        for t in sorted_tracks[:count] if t.get('id') # Ensure ID exists
    ]

    # The source here reflects the source of the *track details*, which might be different
    # from the album ID lookup source. This is handled correctly by returning `source` from `get_album_track_details`.
    return top_tracks_output, source


# --- Playlist Addition Function --- (Refactored slightly for clarity)
def add_tracks_to_playlist(playlist_id, track_ids):
    """Adds a list of track IDs to a Spotify playlist in batches."""
    if not track_ids:
        return 0 # Guard clause: Nothing to add

    # Ensure IDs are strings and remove potential None/empty values, keep order unique
    track_ids_clean = list(dict.fromkeys([str(tid) for tid in track_ids if tid]))

    if not track_ids_clean:
        return 0 # Guard clause: No valid IDs after cleaning

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
            success = False
            # Continue trying subsequent batches if preferred, or break here

    # Print status on the same line if successful, or newline if errors occurred
    if success:
        print(colorize("OK", Colors.GREEN))
    else:
        print(f"  -> Partial addition: {added_count}/{total_to_add} tracks added.")

    return added_count


# --- Input File Reading Function ---
def read_music_file(filepath):
    """Reads and parses the music file, returning entries and playlist metadata."""
    entries = []
    playlist_title = "New Playlist"
    playlist_description = ""
    valid_entry_count = 0

    if not os.path.exists(filepath):
        print(colorize(f"Error: Music file not found at '{filepath}'", Colors.RED))
        sys.exit(1)

    print(f"Reading {colorize(filepath, Colors.CYAN)}: ", end="")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                type_name, p1, p2, p3 = parse_line(line)

                if type_name == "title": playlist_title = p1
                elif type_name == "url": playlist_description = p2 # p2 holds URL content now
                elif type_name == "song" and p1 and p3: # Song: Name, Album (Optional), Artist
                    entries.append({"input_type": "song", "name": p1, "album": p2, "artist": p3})
                    valid_entry_count += 1
                elif type_name in ["album", "ep", "compilation", "single"] and p1 and p2: # Album-like: Name, Artist
                     # Handle 'single' type from parsing as 'album' for processing, or adjust if needed
                    proc_type = "album" if type_name != "song" else "song" # Remap single/ep etc.
                    entries.append({"input_type": proc_type, "name": p1, "album": None, "artist": p2, "original_input_type": type_name})
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

# --- Entry Processing Function ---
def process_entry(entry, processed_track_ids_this_run):
    """Processes a single entry (song or album) to find a Spotify track ID."""
    track_id_to_add = None
    source = 'N/A'
    track_details_for_print = None # e.g., {'name': 'Track Name'} or {'error': 'Error message'}
    is_duplicate = False
    status_code = '?' # F=Found, N=NotFound, E=Error
    source_code = '?' # C=Cache, A=API

    try:
        if entry['input_type'] == 'song':
            song_name = entry['name']; album_name = entry['album']; artist_name = entry['artist']
            song_result, source = search_song(song_name, album_name, artist_name)
            source_code = 'C' if source == 'CACHE' else 'A'

            if song_result:
                track_id = song_result['id']
                if track_id in processed_track_ids_this_run:
                    is_duplicate = True
                    status_code = 'F' # Found, but duplicate
                    track_details_for_print = {'name': song_result['name'], 'is_duplicate': True}
                else:
                    track_id_to_add = track_id
                    status_code = 'F' # Found and new
                    track_details_for_print = {'name': song_result['name']}
            else:
                status_code = 'N' # Not found

        elif entry['input_type'] == 'album':
            album_name = entry['name']; artist_name = entry['artist']
            album_id, source = search_album(album_name, artist_name)
            source_code = 'C' if source == 'CACHE' else 'A'

            if album_id:
                # Attempt to get top tracks even if album found in cache
                # Pass current run's processed IDs to avoid adding duplicates from albums
                found_tracks_details, track_source = get_top_tracks_from_album(
                    album_id, TRACKS_PER_RELEASE, processed_track_ids_this_run
                )

                # If track details came from API, update source indicator
                if track_source == 'API':
                     source_code = 'A'

                if found_tracks_details:
                    # We only expect one track due to TRACKS_PER_RELEASE = 1
                    track_detail = found_tracks_details[0]
                    track_id = track_detail['id']
                    # Check duplicate *again* here because get_top_tracks filters, but race conditions or logic could exist
                    if track_id in processed_track_ids_this_run:
                         is_duplicate = True
                         status_code = 'F' # Found album, got track, but duplicate
                         track_details_for_print = {'name': track_detail['name'], 'is_duplicate': True}
                    else:
                        track_id_to_add = track_id
                        status_code = 'F' # Found album and new track
                        track_details_for_print = {'name': track_detail['name']}
                else:
                     # Album found, but no suitable track found (e.g., all tracks already added)
                     status_code = 'F' # Still mark album as Found
                     # No specific track to add or display as primary result
            else:
                status_code = 'N' # Album not found

    except Exception as e:
        status_code = 'E' # Error during processing
        track_details_for_print = {'error': str(e)}
        # Keep source_code as determined before error, or '?' if error was early

    return track_id_to_add, status_code, source_code, track_details_for_print, is_duplicate


# --- Output Formatting Function ---
def print_entry_result(index, total, entry, status_code, source_code, details, is_duplicate):
    """Formats and prints the processing result for a single entry."""
    max_index_width = len(str(total))
    index_str = f"[{index:>{max_index_width}}/{total}]"
    entry_name_colored = colorize(f"'{entry['name']}'", Colors.YELLOW)
    artist_name_colored = colorize(entry['artist'], Colors.MAGENTA)

    source_color_map = {'C': Colors.CYAN, 'A': Colors.BLUE, '?': Colors.RED}
    status_color_map = {'F': Colors.GREEN, 'N': Colors.RED, 'E': Colors.RED, '?': Colors.RED}
    status_text_map = {'F': "Found", 'N': "Not Found", 'E': "Error", '?': "Unknown"}

    source_tag = colorize(f"[{source_code}]", source_color_map.get(source_code, Colors.RED))
    status_tag = colorize(f"[{status_code}]", status_color_map.get(status_code, Colors.RED))

    # Line 1: Index and Status Tags
    print(f"{index_str} {source_tag} {status_tag}")
    # Line 2: Input Entry Info
    print(f"    ├── {entry_name_colored} by {artist_name_colored}")

    # Line 3: Details (Track, Error, or Duplicate Info)
    details_line = "    └── "
    if details:
        if 'error' in details:
            details_line += f"Error: {colorize(details['error'], Colors.RED)}"
        elif 'name' in details:
            track_name = details['name'] # Default terminal color
            if is_duplicate:
                 details_line += f"Track: '{track_name}' ({colorize('Duplicate', Colors.YELLOW)})"
            else:
                 details_line += f"Track: '{track_name}'"
        else:
            # Should have details if status is F, but handle case if not
             details_line += f"Status: {status_text_map.get(status_code)}"

    elif status_code == 'N':
        details_line += colorize("Not found on Spotify.", Colors.YELLOW)
    else:
        # Fallback for unexpected states without details
        details_line += f"Status: {status_text_map.get(status_code)}"

    print(details_line)

    # Single blank line for spacing after each entry's full output
    print()


# --- Main Function --- (Refactored using helper functions)
def main():
    parser = argparse.ArgumentParser(description='Create a Spotify playlist from a structured TXT.')
    parser.add_argument('music_file', nargs='?', default='music.txt', help='Path to the music file (default: music.txt)')
    parser.add_argument('--clear-cache', action='store_true', help='Clear the API cache before running.')
    args = parser.parse_args()

    if args.clear_cache:
        cache_manager.clear_cache()

    try:
        cache_manager.initialize_cache()
        cache_path = os.path.abspath(cache_manager.DB_FILE)
        print(f"Cache initialized at {colorize(cache_path, Colors.CYAN)}.")
    except Exception as e:
        print(colorize(f"FATAL: Could not initialize cache. Error: {e}", Colors.RED)); sys.exit(1)

    # Read music file and get initial data
    entries, playlist_title, playlist_description = read_music_file(args.music_file)

    # Create Playlist
    print(f"Playlist '{colorize(playlist_title, Colors.YELLOW)}' ", end="")
    playlist_id = None
    try:
        playlist = call_with_retry(sp.user_playlist_create, user=user_id, name=playlist_title, public=False, description=playlist_description)
        playlist_id = playlist['id']
        print(f"created: {colorize('OK', Colors.GREEN)} (ID: {colorize(playlist_id, Colors.CYAN)})")
    except Exception as e:
        print(f"{colorize('creation failed', Colors.RED)}: {e}"); sys.exit(1)

    # --- Process Entries ---
    track_ids_to_add_batch = []
    processed_track_ids_this_run = set() # Keep track of added IDs *within this run*
    total_added_count_overall = 0
    total_entries = len(entries)

    print("\n--- Processing Entries ---")
    print() # Initial spacing

    for i, entry in enumerate(entries):
        track_id, status, source, details, is_dup = process_entry(entry, processed_track_ids_this_run)

        print_entry_result(i + 1, total_entries, entry, status, source, details, is_dup)

        if track_id: # Only add if a new, non-duplicate track ID was found
            track_ids_to_add_batch.append(track_id)
            processed_track_ids_this_run.add(track_id) # Add to set *after* confirming it's added to batch

        # --- Batch adding ---
        if len(track_ids_to_add_batch) >= BATCH_SIZE:
            print(f"  -> Reached batch size ({BATCH_SIZE}). Adding tracks...")
            added_this_batch = add_tracks_to_playlist(playlist_id, track_ids_to_add_batch)
            total_added_count_overall += added_this_batch
            track_ids_to_add_batch = [] # Reset batch
            print() # Add space after batch operation message

    # --- Add final batch ---
    if track_ids_to_add_batch:
        print(f"  -> Adding final batch of {len(track_ids_to_add_batch)} tracks...")
        added_this_batch = add_tracks_to_playlist(playlist_id, track_ids_to_add_batch)
        total_added_count_overall += added_this_batch
        print() # Add space after final batch operation message


    # --- Final Summary ---
    print(f"--- Summary ---")
    playlist_url = f"https://open.spotify.com/playlist/{playlist_id}"
    print(f"Playlist '{colorize(playlist_title, Colors.YELLOW)}' URL: {colorize(playlist_url, Colors.CYAN)}")
    print(f"{colorize(str(total_entries), Colors.GREEN)} entries processed.")
    print(f"{colorize(str(total_added_count_overall), Colors.GREEN)} unique tracks added to the playlist in this run.")

if __name__ == "__main__":
    main()