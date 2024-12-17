import sqlite3
import unittest
import requests
import os
import json
import csv
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

from http.client import HTTPSConnection
import argparse

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials

os.environ['SPOTIPY_CLIENT_ID'] = '0602d044c7364b779ab5cd9d7b886b24'
os.environ['SPOTIPY_CLIENT_SECRET'] = '88a852c5e4d6468db71c11620d503cca'
os.environ['SPOTIPY_REDIRECT_URL'] = 'https://github.com/erobss/Gremily'

def playlist_songs():
#spotipy info
    client_credentials_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # playlist info
    playlist_id = '4AVA9FWl331tDdLTASUlcI'

    p_results = sp.playlist(playlist_id)
    p_json_string = json.dumps(p_results, indent=4)
    playlist_data = json.loads(p_json_string)

    # song dictionary
    playlist_song_dict = {}
    # adding artists and songs to playlist dictionary
    for song in playlist_data['tracks']['items']:
        artist = song['track']['artists'][0]['name']
        first_title = [song['track']['name']]
        addtl_title = song['track']['name']
        if artist in playlist_song_dict:
            playlist_song_dict[artist].append(addtl_title)
        else:
            playlist_song_dict[artist] = first_title

    print(playlist_song_dict)
    return playlist_song_dict

# web scrape the billboard top 100 (as of dec 16, 2024)

def billboard_100():
    # get the url
    URL = 'https://www.billboard.com/charts/hot-100/'
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    # web scrape into the chart content
    results = soup.find_all('div', class_='o-chart-results-list-row-container')
    # create a dictionary to store artists and songs
    billboard_dict = {}
    for res in results:
        songName = res.find('h3').text.strip()
        artist = res.find('h3').find_next('span').text.strip()
        first_song = [songName]
        if artist in billboard_dict:
            billboard_dict[artist].append(songName)
        else:
            billboard_dict[artist] = first_song

    print(billboard_dict)
    return(billboard_dict)


# CREATE DATABASE

def setUpDatabase(db_name):
  path = os.getcwd()
  conn = sqlite3.connect(path+'/'+db_name)
  cur = conn.cursor()
  return cur, conn

# CREATE PLAYLIST AND ID TABLES
# input: cursor and connection
# output: none
def create_playlist_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS m_playlist (artist_id INTEGER, song TEXT, UNIQUE (song))")
    cur.execute("CREATE TABLE IF NOT EXISTS artist_ids (id INTEGER, artist TEXT, UNIQUE (id))")
    cur.execute("CREATE TABLE IF NOT EXISTS billboard_100 (artist TEXT, song TEXT, UNIQUE (song))")
    conn.commit()

# ADD Playlists to DB
# input: database name, which is spotify.db
# output: none
def add_spotify_playlist(db_filename):
    con = sqlite3.connect(db_filename)
    cur = con.cursor()
    data = playlist_songs()
    playlist = playlist_songs()

    # gathering song data into a list
    p_lst = []
    for artist, songs in playlist.items():
        for i in range(len(songs)):
            song = songs[i]
            tup = (artist, song)
            p_lst.append(tup)

    # creating a list of all artists
    artist_lst = []
    for tup in p_lst:
        artist = tup[0]
        artist_lst.append(artist)

    # creating a unique list of artists
    unique_artists = set(artist_lst)
    artists_lst = list(unique_artists)

    # dictionary with keys as artists and values as ids
    artist_id_dict = {}
    for artist in artists_lst:
        artist_id_dict[artist] = artists_lst.index(artist)
    artist_id_list = list(artist_id_dict.items())

    first_25 = artist_id_list[:25]
    second_25 = artist_id_list[25:50]
    third_25 = artist_id_list[50:75]
    fourth_25 = artist_id_list[75:100]
    # executing only 25 artists and ids at a time
    for tup in first_25:
        artist = tup[0]
        id = tup[1]
        cur.execute('INSERT OR IGNORE INTO artist_ids (id, artist) VALUES (?,?)',(id, artist))
    con.commit()

    for tup in second_25:
        artist = tup[0]
        id = tup[1]
        cur.execute('INSERT OR IGNORE INTO artist_ids (id, artist) VALUES (?,?)',(id, artist))
    con.commit()

    for tup in third_25:
        artist = tup[0]
        id = tup[1]
        cur.execute('INSERT OR IGNORE INTO artist_ids (id, artist) VALUES (?,?)',(id, artist))
    con.commit()

    for tup in fourth_25:
        artist = tup[0]
        id = tup[1]
        cur.execute('INSERT OR IGNORE INTO artist_ids (id, artist) VALUES (?,?)',(id, artist))
    con.commit()


    # inserting playlist
    p_to_25 = p_lst[:25]
    p_to_50 = p_lst[25:50]
    p_to_75 = p_lst[50:75]
    p_to_100 = p_lst[75:100]
    for tup in p_to_25:
        artist = tup[0]
        song = tup[1]
        id = artist_id_dict[artist]
        cur.execute('INSERT OR IGNORE INTO m_playlist (artist_id, song) VALUES (?,?)', (id, song))
    con.commit()
    for tup in p_to_50:
        artist = tup[0]
        song = tup[1]
        id = artist_id_dict[artist]
        cur.execute('INSERT OR IGNORE INTO m_playlist (artist_id, song) VALUES (?,?)', (id, song))
    con.commit()
    for tup in p_to_75:
        artist = tup[0]
        song = tup[1]
        id = artist_id_dict[artist]
        cur.execute('INSERT OR IGNORE INTO m_playlist (artist_id, song) VALUES (?,?)', (id, song))
    con.commit()
    for tup in p_to_100:
        artist = tup[0]
        song = tup[1]
        id = artist_id_dict[artist]
        cur.execute('INSERT OR IGNORE INTO m_playlist (artist_id, song) VALUES (?,?)', (id, song))
    con.commit()

# adding to billboard_100

def add_billboard_100(db_filename):
    con = sqlite3.connect(db_filename)
    cur = con.cursor()

    data = billboard_100()

    billboard_lst = []
    for artist, songs in data.items():
        for i in range(len(songs)):
            song = songs[i]
            tup = (artist, song)
            billboard_lst.append(tup)
    # adding only 25 at a time
    bill_to_25 = billboard_lst[:25]
    bill_to_50 = billboard_lst[25:50]
    bill_to_75 = billboard_lst[50:75]
    bill_to_100 = billboard_lst[75:100]
    for tup in bill_to_25:
        artist = tup[0]
        song = tup[1]
        cur.execute('INSERT OR IGNORE INTO billboard_100 (artist, song) VALUES (?,?)', (artist, song))
    con.commit()
    for tup in bill_to_50:
        artist = tup[0]
        song = tup[1]
        cur.execute('INSERT OR IGNORE INTO billboard_100 (artist, song) VALUES (?,?)', (artist, song))
    con.commit()
    for tup in bill_to_75:
        artist = tup[0]
        song = tup[1]
        cur.execute('INSERT OR IGNORE INTO billboard_100 (artist, song) VALUES (?,?)', (artist, song))
    con.commit()
    for tup in bill_to_100:
        artist = tup[0]
        song = tup[1]
        cur.execute('INSERT OR IGNORE INTO billboard_100 (artist, song) VALUES (?,?)', (artist, song))
    con.commit()


def process_and_write_data(cur, conn, filename="artist_counts.txt"):
    """
    Processes the data to calculate:
    1. How many times each artist appears in the Billboard Hot 100.
    2. How many times each artist appears in the Spotify playlist.
    3. How many times each artist appears in both Billboard and Playlist (overlap).
    Writes the results to a single text file.

    Input: Cursor, connection, filename
    Output: None
    """
    # count how many times each artist is in Billboard Hot 100
    query_billboard = """
    SELECT artist, COUNT(*) AS count
    FROM billboard_100
    GROUP BY artist
    ORDER BY count DESC;
    """
    cur.execute(query_billboard)
    billboard_results = cur.fetchall()

    # count how many times each artist in playlist
    query_playlist = """
    SELECT a.artist, COUNT(*) AS count
    FROM m_playlist m
    JOIN artist_ids a ON m.artist_id = a.id
    GROUP BY a.artist
    ORDER BY count DESC;
    """
    cur.execute(query_playlist)
    playlist_results = cur.fetchall()

    # count overlapping songs (artists and songs in both Billboard and Playlist)
    query_overlap = """
    SELECT a.artist, COUNT(*) AS count
    FROM m_playlist m
    JOIN artist_ids a ON m.artist_id = a.id
    JOIN billboard_100 b ON a.artist = b.artist AND m.song = b.song
    GROUP BY a.artist
    ORDER BY count DESC;
    """
    cur.execute(query_overlap)
    overlap_results = cur.fetchall()

    # Write results to a text file
    with open(filename, "w") as f:
        f.write("Artist Appearance Counts\n")
        f.write("=" * 50 + "\n\n")

        # Billboard results
        f.write("Billboard Hot 100 Artist Counts:\n")
        f.write("-" * 50 + "\n")
        for artist, count in billboard_results:
            f.write(f"{artist}: {count} times\n")

        # Playlist results
        f.write("\nSpotify Playlist Artist Counts:\n")
        f.write("-" * 50 + "\n")
        for artist, count in playlist_results:
            f.write(f"{artist}: {count} times\n")

        # Overlap results
        f.write("\nArtists Appearing in Both Billboard and Playlist:\n")
        f.write("-" * 50 + "\n")
        for artist, count in overlap_results:
            f.write(f"{artist}: {count} songs in both\n")


    print(f"Artist counts written to {filename}")

    print("\nBillboard Hot 100 Artist Counts:")
    for artist, count in billboard_results:
        print(f"{artist}: {count}")

    print("\nSpotify Playlist Artist Counts:")
    for artist, count in playlist_results:
        print(f"{artist}: {count}")

    print("\nArtists Appearing in Both Billboard and Playlist:")
    for artist, count in overlap_results:
        print(f"{artist}: {count}")


def visualize_data(cur, conn):
    """
    Creates 4 visualizations:
    1. Proportion of Billboard songs that are also in the Spotify playlist.
    2. Count of overlapping songs by artist.
    3. Top artists in Billboard Hot 100.
    4. Top artists in Spotify playlist.
    Input: Cursor, connection
    Output: None
    """
    # Visualization 1 Proportion of billboard songs in playlist
    query_overlap = """
    SELECT COUNT(*)
    FROM m_playlist m
    JOIN artist_ids a ON m.artist_id = a.id
    JOIN billboard_100 b ON a.artist = b.artist AND m.song = b.song
    """
    cur.execute(query_overlap)
    overlap_count = cur.fetchone()[0]

    query_total_billboard = "SELECT COUNT(*) FROM billboard_100"
    cur.execute(query_total_billboard)
    total_billboard_count = cur.fetchone()[0]

    plt.figure(figsize=(6, 6))
    labels = ['In Both Billboard & Playlist', 'Only in Billboard']
    sizes = [overlap_count, total_billboard_count - overlap_count]
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title("Proportion of Billboard Songs in Spotify Playlist")
    plt.show()

    # Visualization 2 count of songs in both by artist
    query_artist_overlap = """
    SELECT a.artist, COUNT(*) AS count
    FROM m_playlist m
    JOIN artist_ids a ON m.artist_id = a.id
    JOIN billboard_100 b ON a.artist = b.artist AND m.song = b.song
    GROUP BY a.artist
    ORDER BY count DESC
    LIMIT 10
    """
    cur.execute(query_artist_overlap)
    artist_overlap_results = cur.fetchall()
    artists = [item[0] for item in artist_overlap_results]
    counts = [item[1] for item in artist_overlap_results]

    plt.figure(figsize=(10, 6))
    plt.bar(artists, counts, color='lightblue')
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Artists")
    plt.ylabel("Number of Overlapping Songs")
    plt.title("Top Artists with Songs in Both Billboard and Playlist")
    plt.tight_layout()
    plt.show()

    # Visualization 3 top artists in billboard
    query_top_billboard = """
    SELECT artist, COUNT(*) AS count
    FROM billboard_100
    GROUP BY artist
    ORDER BY count DESC
    LIMIT 10
    """
    cur.execute(query_top_billboard)
    top_billboard_results = cur.fetchall()
    artists = [item[0] for item in top_billboard_results]
    counts = [item[1] for item in top_billboard_results]

    plt.figure(figsize=(10, 6))
    plt.bar(artists, counts, color='salmon')
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Artists")
    plt.ylabel("Number of Songs")
    plt.title("Top Artists in Billboard Hot 100")
    plt.tight_layout()
    plt.show()

    # Visualization 4 top artists in playlist
    query_top_playlist = """
    SELECT a.artist, COUNT(*) AS count
    FROM m_playlist m
    JOIN artist_ids a ON m.artist_id = a.id
    GROUP BY a.artist
    ORDER BY count DESC
    LIMIT 10
    """
    cur.execute(query_top_playlist)
    top_playlist_results = cur.fetchall()
    artists = [item[0] for item in top_playlist_results]
    counts = [item[1] for item in top_playlist_results]

    plt.figure(figsize=(10, 6))
    plt.bar(artists, counts, color='lightgreen')
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Artists")
    plt.ylabel("Number of Songs")
    plt.title("Top Artists in Spotify Playlist")
    plt.tight_layout()
    plt.show()



def main():
    cur, conn = setUpDatabase('Spotify.db')
    create_playlist_table(cur, conn)

    add_spotify_playlist('Spotify.db')

    add_billboard_100('Spotify.db')

    process_and_write_data(cur, conn, filename="artist_counts.txt")

    visualize_data(cur, conn)

    conn.close()


# class TestDiscussion12(unittest.TestCase):
#     def setUp(self) -> None:
#         self.cur, self.conn = setUpDatabase('Spotify.db')

if __name__ == "__main__":
    main()