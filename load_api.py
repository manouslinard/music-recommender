import requests
import json
import re
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import create_db
from dotenv import load_dotenv
import os

load_dotenv()

YOUR_API_KEY = os.getenv("YOUR_API_KEY")

band_names = ["coldplay", "scorpions", "the+beatles", "queen", "acdc", "u2"]

def find_info_band(band_name: str) -> dict:
    band_url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={band_name}&api_key={YOUR_API_KEY}&format=json"
    response = requests.get(band_url)
    json_data = json.loads(response.text)
    artist_name = json_data['artist']['name']
    artist_summary = re.sub('<a.*?>.*?</a>', '', json_data['artist']['bio']['summary'])
    return {"name":artist_name, "summary": artist_summary}


def find_top_albums(band_name: str) -> list:
    alb = []
    album_url = f"http://ws.audioscrobbler.com/2.0/?method=artist.gettopalbums&artist={band_name}&api_key={YOUR_API_KEY}&format=json"
    response = requests.get(album_url)
    top_albums = json.loads(response.text)['topalbums']['album']
    for a in top_albums:
        alb.append(a["name"].replace("'", "_"))
    return alb

def load_api():

    print("Collecting requested band data...")
    albums_bands = []
    for b in band_names:
        r = find_info_band(b)
        a = find_top_albums(b)
        r["albums"] = a
        albums_bands.append(r)

    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=os.getenv("PSQL_HOST"),
        database=os.getenv("PSQL_DATABASE"),
        user=os.getenv("PSQL_USERNAME"),
        password=os.getenv("PSQL_PASSWORD")
    )

    # Set isolation level to AUTOCOMMIT
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    create_db.create_tables(conn)

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Iterate through the data and insert each item into the PostgreSQL table
    for item in albums_bands:
        name = item['name']
        summary = item['summary'].replace('\n', ' ')
        # summary = "hello"
        albums = item['albums']
        cur.execute("INSERT INTO Bands (name, summary) VALUES (%s, %s)", (name, summary))
        for a in albums:
            if a != "(null)":
                cur.execute("INSERT INTO Discs (name, band) VALUES (%s, %s)", (a, name))


    create_db.load_users(conn)
    create_db.fill_barabasi_model(conn)
    create_db.insert_user_has_disc(conn)
    create_db.insert_user_likes_band(conn)

    # Commit the transaction and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()

    # print(r)

if __name__ == "__main__":
    load_api()
