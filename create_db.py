import random
import os
import pandas as pd
from scipy.stats.mstats import winsorize
import networkx as nx
import matplotlib.pyplot as plt
import scraper.scrape as sp
from Crypto.Cipher import AES
from dotenv import load_dotenv

import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

def create_tables(conn):
    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Create the tables if they do not already exist
        create_table_commands = (
            """
            CREATE TABLE IF NOT EXISTS Users (
                username VARCHAR(50) PRIMARY KEY,
                password TEXT NOT NULL,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                gender CHAR,
                country VARCHAR(50),
                age INTEGER,
                phone VARCHAR(100) NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Bands (
                name VARCHAR(50) PRIMARY KEY,
                summary TEXT,
                band_id INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Discs (
                name VARCHAR(250) NOT NULL,
                band VARCHAR(50) NOT NULL,
                PRIMARY KEY (name, band),
                FOREIGN KEY (band) REFERENCES Bands (name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_has_discs (
                username VARCHAR(50),
                disc_name VARCHAR(250),
                disc_band VARCHAR(50),
                CONSTRAINT pk_user_has_discs PRIMARY KEY (username, disc_name, disc_band),
                CONSTRAINT fk_user_has_discs_username FOREIGN KEY (username)
                    REFERENCES users (username),
                CONSTRAINT fk_user_has_discs_disc FOREIGN KEY (disc_name, disc_band)
                    REFERENCES discs (name, band)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_likes_band (
                username VARCHAR(50),
                band_name VARCHAR(50),
                CONSTRAINT pk_user_likes_band PRIMARY KEY (username, band_name),
                CONSTRAINT fk_user_likes_band_username FOREIGN KEY (username)
                    REFERENCES users (username),
                CONSTRAINT fk_user_likes_band_name FOREIGN KEY (band_name)
                    REFERENCES Bands (name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS User_Friends (
                username VARCHAR(50) NOT NULL,
                friend_username VARCHAR(50) NOT NULL,
                PRIMARY KEY (username, friend_username),
                FOREIGN KEY (username) REFERENCES Users (username),
                FOREIGN KEY (friend_username) REFERENCES Users (username)
            )
            """,
            
            """
            CREATE TABLE IF NOT EXISTS disc_prices (
                date DATE NOT NULL,
                values FLOAT,
                name VARCHAR(250) NOT NULL,
                band VARCHAR(50) NOT NULL,
                FOREIGN KEY (name,band) REFERENCES Discs (name,band)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_rec_discs (
                username VARCHAR(50),
                disc_name VARCHAR(250),
                disc_band VARCHAR(50),
                CONSTRAINT pk_user_rec_discs PRIMARY KEY (username, disc_name, disc_band),
                CONSTRAINT fk_user_rec_discs_username FOREIGN KEY (username)
                    REFERENCES users (username),
                CONSTRAINT fk_user_rec_discs_disc FOREIGN KEY (disc_name, disc_band)
                    REFERENCES discs (name, band)
            )
            """
        )
        foreign_keys = """
        ALTER TABLE Discs
        ADD CONSTRAINT fk_band
        FOREIGN KEY (band) REFERENCES Bands(name);
        """
        
        # Execute the CREATE TABLE commands
        for command in create_table_commands:
            cursor.execute(command)
        # Commit the changes to the database
        conn.commit()
        # Execute the ALTER TABLE command to add foreign key constraint
        cursor.execute(foreign_keys)

        # Commit the changes to the database
        conn.commit()

        print("Foreign key added successfully.")

    except Exception as e:
        print(f"Error: {e}")

def load_users(conn):
    # Create a cursor object
    cursor = conn.cursor()

    # Open the CSV file and read the data
    df = pd.read_csv("users.csv")
    if df['username'].isnull().any():
        raise ValueError("Missing username in the CSV file.")
    if df['password'].isnull().any():
        raise ValueError("Missing password in the CSV file.")
    if df['age'].min() <= 10:
        raise ValueError("Invalid age in the CSV file (users should be above 10 years old).")
    if df['age'].max() > 110:
        raise ValueError("Invalid age in the CSV file (user aged over 110 years old).")
    if df['gender'].isin(['N']).any():
        raise ValueError("Invalid gender in the CSV file (should be M or F).")
    df['gender'].fillna("N", inplace=True)
    if not df['gender'].isin(['M', 'F', 'N']).all():
        raise ValueError("Invalid gender in the CSV file (should be M or F).")
    df['age'].fillna(-1, inplace=True)
    df.fillna("unregistered", inplace=True)

    cursor.executemany("INSERT INTO users (username, password, first_name, last_name, phone, gender, country, age) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                   [(row['username'], encr(row['password']), row['first_name'], row['last_name'], row['phone'], row['gender'], row['country'], row['age']) for index, row in df.iterrows()])

    conn.commit()
    print("Data inserted successfully.")

def encr(password):
    load_dotenv()
    key = os.getenv('SECRET_KEY', '1234567890123456').encode('utf-8')
    # Input string to be encrypted (padding to adjust length)
    input_string = password.rjust(32)
    # Encrypt the string
    cipher = AES.new(key)
    return cipher.encrypt(input_string.encode())

def load_prices_webscrape(conn, MAX_DISCS=-1):
    cursor = conn.cursor()
    cursor.execute("SELECT band_id,discs.band,discs.name FROM discs JOIN bands ON discs.band=bands.name")
    rows = cursor.fetchall()
    count = 0
    for row in rows:
        if MAX_DISCS > 0 and count >= MAX_DISCS:
            break
        count += 1
        band_id, band_name, disc_name = row
        d = sp.load_prices_discogs(band_id, disc_name)
        # define the query to insert the dictionary into the database table
        # print(d)
        if not d.empty and d.size >= 7: # if d.size < 7 then we are adding the synthetic data
            # define the query to insert the dataframe into the database table
            insert_query = "INSERT INTO disc_prices (name, values, band, date) VALUES (%s, %s, %s, %s)"
            # iterate over the dataframe and insert each row into the database
            for index, row in d.iterrows():
                cursor.execute(insert_query, (disc_name, row["lowest_price"], band_name, index.date()))
        else:
            print("Loading synthetic data...")
            load_prices(conn,disc_name,band_name)

def load_prices(conn,name_of_disc:str = None, band_of_disk:str= None):
    cursor = conn.cursor()
    cursor.execute("SELECT name, band FROM discs")
    discs = cursor.fetchall()
    df = pd.DataFrame(discs, columns=['name', 'band'])
    if  name_of_disc  and band_of_disk :
        # Check if the disc and band exist in the database
        cursor.execute("SELECT * FROM discs WHERE name = %s AND band = %s", (name_of_disc, band_of_disk))
        disc = cursor.fetchone()
        if disc:
            prices_insertion(conn,cursor,df,name_of_disc,band_of_disk)
            return

    for disc in discs:
        name, band = disc
        prices_insertion(conn,cursor,df,name,band)
    

def prices_insertion(conn,cursor,df,name,band):
    df = pd.read_csv("File_series.csv")
    df['date'] = pd.to_datetime(df['date'])

    # Get the index of the row with the minimum date
    min_date_index = df['date'].idxmin()

    # Get the values of the row with the minimum date
    min_date_values = df.iloc[min_date_index]['date']

    # if the first date is missing, replace it with the earliest date from the database
    if pd.isna(df['date'][0]):
        df.loc[0, 'date'] = min_date_values - pd.Timedelta(min_date_index, unit='D')

    # fill missing dates with the previous date + 1
    df['date'] = df['date'].fillna(method='ffill') + pd.to_timedelta(df.groupby(df['date'].ffill()).cumcount(), unit='D')

    # fill missing values with rolling mean and forward fill
    df['values'] = df['values'].fillna(df['values'].rolling(window=len(df), min_periods=1, center=False).mean())
    df['values'] = df['values'].ffill()
    # apply winsorize to each column separately
    for col in df.columns:
        if col != "date":
            df[col] = winsorize(df[col], limits=(0.01, 0.02))
    for row in df.itertuples(index=False):
        cursor.execute("INSERT INTO disc_prices (date, values, name, band) VALUES (%s, %s, %s, %s)", (row.date, row.values, name, band))
    conn.commit()
    print(f"Prices for disc {name} of band {band} inserted successfully.")


def insert_user_has_disc(conn):
    cur = conn.cursor()

    # Select all usernames from the users table
    cur.execute("SELECT username FROM users")
    users = cur.fetchall()

    # Select all disc names and band names from the discs table
    cur.execute("SELECT name, band FROM discs")
    discs = cur.fetchall()

    # Insert random disc ownerships for each user
    for user in users:
        for i in range(random.randint(0, 5)):
            disc = random.choice(discs)
            insert_query = "INSERT INTO user_has_discs VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
            cur.execute(insert_query, (user[0], disc[0], disc[1]))
    print("Initialized relation user-has-disc.")
    conn.commit()


def insert_user_likes_band(conn):
    cur = conn.cursor()

    # Select all usernames from the users table
    cur.execute("SELECT username FROM users")
    users = cur.fetchall()

    # Select all disc names and band names from the discs table
    cur.execute("SELECT name FROM bands")
    bands = cur.fetchall()

    # Insert random disc ownerships for each user
    for user in users:
        for i in range(random.randint(0, 5)):
            band = random.choice(bands)
            insert_query = f"INSERT INTO user_likes_band VALUES ('{user[0]}', '{band[0]}') ON CONFLICT DO NOTHING"
            cur.execute(insert_query)
    print("Initialized relation user-likes-band.")
    conn.commit()

def fill_barabasi_model(conn, m=3):
    """
    Define the parameters of the Barabási-Albert model.
    Parameters: m -> number of edges to attach from a new node to existing nodes
    """
    # Get the usernames from the database and saves them to a list.
    query = """
        SELECT Users.username, user_has_discs.disc_name, user_has_discs.disc_band FROM Users
        LEFT JOIN user_has_discs ON Users.username=user_has_discs.username
    """
    df = pd.read_sql(query, conn)
    usernames = df['username'].unique()
    # print(df['username'])

    n = len(usernames)
    # print(n)
    community_graph = nx.barabasi_albert_graph(n, m)

    G = nx.Graph()
    edges = [(usernames[e[0]], usernames[e[1]]) for e in community_graph.edges]
    G.add_edges_from(edges)
    # nx.draw(G, with_labels=True)
    # plt.show()

    with conn.cursor() as cur:
        for friends in G.edges:
            cur.execute("INSERT INTO User_Friends (username, friend_username) VALUES (%s, %s)", (friends[0], friends[1]))
        conn.commit()
    print("Filled User-Friends Table according to Barabasi Model.")

    # adding discs feature:
    for user in G.nodes:
        node_data = df[df['username'] == user]
        discs = node_data['disc_name'].tolist()
        bands = node_data['disc_band'].tolist()
        G.nodes[user]['discs'] = list(zip(discs, bands))

    load_dotenv()
    print("Recommending discs...")
    for user in G.nodes:
        d = {}
        neighbors = list(G.adj[user].keys())
        # print((user, d))
        for n in neighbors:
            discs = G.nodes[n]['discs']
            for disc in discs:
                if disc[0] and disc not in d and disc not in G.nodes[user]['discs']:
                    print(disc, G.nodes[user]['discs'])
                    d[disc] = 1
                elif disc[0] and disc not in G.nodes[user]['discs']: # recommends only the discs that the user does not have.
                    print(disc, G.nodes[user]['discs'])
                    d[disc] += 1
        sorted_list = sorted(d.items(), key=lambda x: x[1], reverse=True)
        # print(user, sorted_list)
        rec_discs = int(os.getenv("NUMBER_REC_DISCS", 1))
        insert_query = "INSERT INTO user_rec_discs (username, disc_name, disc_band) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
        with conn.cursor() as cur:
            for d in sorted_list[:rec_discs]: # gets the top rec_discs+1 dics.
                cur.execute(insert_query, (user, d[0][0], d[0][1]))
        conn.commit()
    print("Discs recommended.")

if __name__ == "__main__":
    print("This file is not executable and contains methods used in load_api.py. To run the project, run the load_api.py file.")
    # conn = psycopg2.connect(
    #     host=os.getenv("PSQL_HOST"),
    #     database=os.getenv("PSQL_DATABASE"),
    #     user=os.getenv("PSQL_USERNAME"),
    #     password=os.getenv("PSQL_PASSWORD")
    # )
    # fill_barabasi_model(conn)
