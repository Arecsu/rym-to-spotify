import sqlite3
import time
import json
import hashlib
import os
from contextlib import contextmanager

DB_FILE = 'spotify_cache.db'
# 6 months expiry time in seconds (approx)
CACHE_EXPIRY_SECONDS = 6 * 30 * 24 * 60 * 60
NOT_FOUND_MARKER = '__NOT_FOUND__' # Marker for results confirmed not found
# New query type for storing all track details of an album
ALBUM_DETAILS_TYPE = 'album_full_details'


@contextmanager
def get_db_connection():
    """Provides a managed database connection."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.row_factory = sqlite3.Row
        yield conn
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def initialize_cache():
    """Creates the cache table if it doesn't exist."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # No changes needed to table structure itself
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_cache (
                    query_hash TEXT PRIMARY KEY,
                    query_type TEXT NOT NULL,
                    result TEXT, -- Store result as JSON or marker
                    timestamp INTEGER NOT NULL,
                    query_params TEXT -- Store params as JSON for debugging
                )
            ''')
            conn.commit()
        print("Cache initialized.")
    except sqlite3.Error as e:
        print(f"Failed to initialize cache database: {e}")
        raise

def clear_cache():
    """Deletes the cache database file."""
    if os.path.exists(DB_FILE):
        try:
            os.remove(DB_FILE)
            print("Cache file deleted.")
        except OSError as e:
            print(f"Error deleting cache file {DB_FILE}: {e}")
    else:
        print("Cache file not found, nothing to delete.")

def _generate_query_hash(query_type: str, params_dict: dict) -> str:
    """Generates a SHA-256 hash for a given query type and parameters."""
    # For album details, the key params might just be the album_id
    # For others, use the sorted dict approach
    if query_type == ALBUM_DETAILS_TYPE and 'album_id' in params_dict:
         # Simple hash based only on album_id for this specific type
         hash_input = f"{query_type.lower().strip()}|album_id:{params_dict['album_id']}"
    else:
        # Original method for other types (search_song, search_album)
        sorted_params = sorted(params_dict.items())
        normalized_params = []
        for k, v in sorted_params:
            key_norm = str(k).lower().strip()
            if isinstance(v, str): val_norm = v.lower().strip()
            elif isinstance(v, (list, set, tuple)):
                try: val_norm = json.dumps(sorted([str(item).lower().strip() for item in v]), sort_keys=True)
                except TypeError: val_norm = json.dumps([str(item).lower().strip() for item in v], sort_keys=True)
            elif v is None: val_norm = 'none'
            else: val_norm = str(v).lower().strip()
            normalized_params.append(f"{key_norm}:{val_norm}")
        hash_input = f"{query_type.lower().strip()}|{'|'.join(normalized_params)}"

    # print(f"DEBUG: Hash input: {hash_input}") # Uncomment for debugging
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

def check_cache(query_type: str, params_dict: dict):
    """Checks the cache for a valid, non-expired entry."""
    query_hash = _generate_query_hash(query_type, params_dict)
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT result, timestamp FROM api_cache WHERE query_hash = ?",
                (query_hash,)
            )
            row = cursor.fetchone()

            if row:
                cached_result_str = row['result']
                timestamp = row['timestamp']
                current_time = int(time.time())
                age = current_time - timestamp

                if age < CACHE_EXPIRY_SECONDS:
                    # print(f"[CACHE HIT] Found valid entry for {query_type} (Hash: {query_hash[:8]}...).")
                    if cached_result_str == NOT_FOUND_MARKER:
                        # Specific handling for album_details if needed, maybe return empty list?
                        return NOT_FOUND_MARKER if query_type != ALBUM_DETAILS_TYPE else []
                    try:
                        # Always try to parse as JSON first
                        return json.loads(cached_result_str)
                    except json.JSONDecodeError:
                         # If it's not JSON, return the raw string (likely an ID for search_song/album)
                        return cached_result_str
                else:
                    print(f"[CACHE STALE] Entry found but expired for {query_type} (Hash: {query_hash[:8]}...).")
                    return None # Stale entry
            else:
                return None # No entry found
    except sqlite3.Error as e:
        print(f"Error checking cache: {e}")
        return None # Treat DB errors as cache misses

def update_cache(query_type: str, params_dict: dict, result):
    """Adds or updates an entry in the cache."""
    query_hash = _generate_query_hash(query_type, params_dict)
    current_time = int(time.time())
    # Store only essential params for album_details key debug
    if query_type == ALBUM_DETAILS_TYPE:
         params_to_store = {'album_id': params_dict.get('album_id')}
    else:
         params_to_store = params_dict
    params_json = json.dumps(params_to_store, sort_keys=True)

    # Serialize result for storage
    if result is None or result == NOT_FOUND_MARKER:
         # For album details, store empty list marker ('[]') instead of NOT_FOUND?
         # Or stick to NOT_FOUND? Let's stick to NOT_FOUND for consistency,
         # but return empty list in check_cache maybe. Or store '[]' directly.
         # Let's store '[]' for empty albums/failed fetches for ALBUM_DETAILS_TYPE
        result_str = '[]' if query_type == ALBUM_DETAILS_TYPE and result == NOT_FOUND_MARKER else NOT_FOUND_MARKER

    elif isinstance(result, (list, dict)):
        # This will now handle the list of track details for ALBUM_DETAILS_TYPE
        result_str = json.dumps(result, sort_keys=True)
    else:
        result_str = str(result) # Store simple IDs as strings

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO api_cache
                (query_hash, query_type, result, timestamp, query_params)
                VALUES (?, ?, ?, ?, ?)
                """,
                (query_hash, query_type, result_str, current_time, params_json)
            )
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error updating cache: {e}")