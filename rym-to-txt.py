#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#     "requests",
#     "beautifulsoup4",
#     "nodriver"
# ]
# ///

import sys
import re
import time
import asyncio

from urllib.parse import urljoin
from bs4 import BeautifulSoup
from nodriver import start, Browser, Tab
from nodriver.core.connection import ProtocolException


def sanitize_filename(filename):
    """Remove invalid characters for filenames."""
    return re.sub(r'[\\/*?:"<>|]', "", filename).strip()


def clean_song_name(song, artist, album):
    """
    Clean up the song title by:
      - Removing occurrences of the artist and album names,
      - Removing unwanted phrases and patterns like official video tags, 
        full album/compilation markers, lyrics, audio hints, etc.
      - Trimming extra punctuation and whitespace.
    """
    original = song  # For debugging if needed.

    # Remove occurrences of artist and album names (case-insensitive)
    song = re.sub(re.escape(artist), "", song, flags=re.IGNORECASE)
    song = re.sub(re.escape(album), "", song, flags=re.IGNORECASE)

    # Remove content in parentheses or braces that contain unwanted keywords.
    unwanted_in_brackets = [
        r'\bofficial\b', r'\bremaster(ed)?\b', r'\bvideo\b',
        r'\baudio\b', r'\bclip\b', r'\blyrics\b', r'\bfull\s*album\b',
        r'\bcompilation\b'
    ]
    # Build a regex pattern that finds any unwanted keyword inside parentheses/braces.
    pattern = r'[\(\{][^)\}]*(' + '|'.join(unwanted_in_brackets) + r')[^)\}]*[\)\}]'
    song = re.sub(pattern, '', song, flags=re.IGNORECASE)

    # Remove unwanted phrases outside brackets.
    unwanted_phrases = [
        "official video", "official music video", "video clip", "video official",
        "remastered", "official audio", "full album", "compilation", "lyrics", "audio"
    ]
    for phrase in unwanted_phrases:
        song = re.sub(re.escape(phrase), '', song, flags=re.IGNORECASE)

    # Remove extraneous quotation marks
    song = song.replace('"', '').replace("''", '')

    # Remove extra separator characters (hyphens, colons, etc.) at the beginning or end.
    song = re.sub(r"^[\s\-–:]+", "", song)
    song = re.sub(r"[\s\-–:]+$", "", song)

    # Replace multiple spaces with a single space.
    song = re.sub(r"\s{2,}", " ", song)

    return song.strip()


async def wait_for_selector(tab, selector):
    await tab.wait(40)  # Simple fallback: wait 40 seconds


async def scrape_rym_list(url):
    if not url.startswith('https://rateyourmusic.com/'):
        print("Error: Please provide a valid RateYourMusic URL")
        sys.exit(1)

    base_url = url
    results = []
    current_url = base_url
    output_filename = None  # Will be set after we get the title

    browser: Browser = await start(no_sandbox=True)
    tab: Tab = await browser.get(current_url)

    title_extracted = False

    while True:
        await wait_for_selector(tab, '#user_list')

        try:
            page_source = await tab.get_content()
        except ProtocolException:
            # Fallback: sometimes using evaluate is more robust for dynamic pages.
            await asyncio.sleep(1)
            page_source = await tab.evaluate("document.documentElement.outerHTML")

        soup = BeautifulSoup(page_source, 'html.parser')
        list_table = soup.find('table', id='user_list')
        if not list_table:
            continue
            print("Table #user_list not found on page. Stopping.")
            break

        if not title_extracted:
            header_tag = soup.find('h1')
            list_title = header_tag.get_text().strip() if header_tag else "Unknown"
            user_tag = soup.find('a', class_='user')
            username = user_tag.get_text().strip() if user_tag else "Unknown"

            output_filename = sanitize_filename(f"{list_title} - {username}.txt")
            results.append(f'title: "{list_title} - {username}"')
            results.append(f'url: "{base_url}"')
            title_extracted = True

        list_items = list_table.find_all(
            'tr',
            class_=lambda c: c and ('trodd' in c or 'treven' in c)
        )

        for item in list_items:
            # Skip rows that are description only.
            if 'list_mobile_description' in item.get('class', []):
                continue
            if item.find(class_='generic_item'):
                continue

            # Extract artist from the h2 tag.
            artist_tag = item.find('h2')
            if not artist_tag:
                continue
            artist_link = artist_tag.find('a', class_='list_artist')
            if not artist_link:
                continue
            artist = artist_link.get_text().strip()

            # Extract title (album/EP/etc.) from the h3 tag.
            title_tag = item.find('h3')
            if not title_tag:
                continue
            title_link = title_tag.find('a', class_='list_album')
            if not title_link:
                continue
            album_title = title_link.get_text().strip()
            href = title_link.get('href', '')

            # Determine item type based on href and additional release info.
            if '/release/' in href:
                rel_date_tag = title_tag.find('span', class_='rel_date')
                if rel_date_tag:
                    type_info = rel_date_tag.get_text().strip()  # e.g. "(2020) [Compilation]"
                    if "[EP]" in type_info:
                        item_type = "EP"
                    elif "[Compilation]" in type_info:
                        item_type = "Compilation"
                    elif "[Single]" in type_info:
                        item_type = "Single"
                    else:
                        item_type = "Album"
                else:
                    item_type = "Album"
            else:
                continue  # Skip if not a release

            # Process song link extraction and cleaning.
            song_link = None
            for a in item.find_all('a'):
                if a == artist_link or a == title_link:
                    continue
                href_candidate = a.get('href', '')
                if any(domain in href_candidate for domain in ['youtube.com', 'spotify.com', 'bandcamp.com']):
                    song_link = a
                    break

            if song_link:
                # Try to extract the song title from a specific sub-element if available.
                youtube_title_elem = song_link.find('div', class_='youtube_title')
                if youtube_title_elem:
                    song_text = youtube_title_elem.get_text().strip()
                else:
                    # Fallback: use the full anchor text.
                    song_text = song_link.get_text(separator=" ", strip=True)
                    # Optionally remove known extra words that might appear.
                    unwanted = ["Listen", "video clip", "video official", "remastered", "audio", "lyrics"]
                    for word in unwanted:
                        song_text = song_text.replace(word, "")
                    song_text = song_text.strip()

                # Clean the extracted text by removing the artist and album names and unwanted patterns.
                song_name = clean_song_name(song_text, artist, album_title)

                # If the cleaned song name is empty or contains patterns that suggest it's not a song,
                # output the entry as an album instead.
                if not song_name or any(x in song_text.lower() for x in ["full album", "compilation", "album only"]):
                    results.append(f'album: "{album_title}" - "{artist}"')
                else:
                    results.append(f'song: "{song_name}" - "{album_title}" - "{artist}"')
            else:
                results.append(f'{item_type.lower()}: "{album_title}" - "{artist}"')

        next_link = soup.find('a', class_='navlinknext')
        if next_link:
            next_href = next_link.get('href', '')
            current_url = urljoin(base_url, next_href)
            print(f"Moving to next page: {current_url}")
            await tab.get(current_url)
            await asyncio.sleep(1)
        else:
            print("No next page found. Scraping complete.")
            break

    if output_filename:
        with open(output_filename, 'w', encoding='utf-8') as f:
            for item in results:
                f.write(item + '\n')
        print(f"Scraping complete. {len(results)} items written to {output_filename}")
    else:
        print("Error: Could not determine output filename")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python rym-to-txt.py <RYM_URL>")
        print("Example: python rym-to-txt.py https://rateyourmusic.com/list/M4rcus/dream-folk/")
        sys.exit(1)

    rym_url = sys.argv[1]
    asyncio.run(scrape_rym_list(rym_url))
