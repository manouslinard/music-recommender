import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import csv
import pandas as pd
import numpy as np


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
