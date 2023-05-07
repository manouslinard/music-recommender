import random
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

def create_tables(conn):
    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Create the tables if they do not already exist
        create_table_commands = (
            """
            CREATE TABLE IF NOT EXISTS Users (
                username VARCHAR(50) PRIMARY KEY,
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
                summary TEXT
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

    cursor.executemany("INSERT INTO users (username, first_name, last_name, phone, gender, country, age) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                   [(row['username'], row['first_name'], row['last_name'], row['phone'], row['gender'], row['country'], row['age']) for index, row in df.iterrows()])

    conn.commit()
    print("Data inserted successfully.")

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
            insert_query = f"INSERT INTO user_has_discs VALUES ('{user[0]}', '{disc[0]}', '{disc[1]}') ON CONFLICT DO NOTHING"
            cur.execute(insert_query)
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
    # create a cursor object
    cur = conn.cursor()
    # Get the usernames from the database and saves them to a list.
    cur.execute("SELECT username FROM Users")
    usernames = [row[0] for row in cur.fetchall()]
    n = len(usernames)  # number of nodes in barabasi model.
    # print(n)
    # generate the graph
    community_graph = nx.barabasi_albert_graph(n, m)

    mapping = {}
    # maps nodes to usernames.
    for node, u in zip(community_graph, usernames):
        mapping[node] = u
    # DEBUG =================================================
    # print(mapping)
    # print("=============")
    # print(community_graph.edges)
    # nx.draw(community_graph, with_labels=True)
    # plt.show()
    # ======================================================
    # Insert the edges into the User_Friends table
    for edge in community_graph.edges:
        user1 = mapping[edge[0]]
        user2 = mapping[edge[1]]
        cur.execute("INSERT INTO User_Friends (username, friend_username) VALUES (%s, %s)", (user1, user2))
    print("Filled User-Friends Table according to Barabasi Model.")
    # Commit the changes to the database
    conn.commit()

if __name__ == "__main__":
    print("This file is not executable and contains methods used in load_api.py. To run the project, run the load_api.py file.")
