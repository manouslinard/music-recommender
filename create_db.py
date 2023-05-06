import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import config # a separate config file with database credentials

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=config.DATABASE,
    user=config.USERNAME,
    password=config.PASSWORD,
    host=config.HOST,
    port=config.PORT
)

try:
    # Set isolation level to AUTOCOMMIT
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Create a cursor object
    cursor = conn.cursor()

    # Create the tables if they do not already exist
    create_table_commands = (
        """
        CREATE TABLE IF NOT EXISTS Users (
            username VARCHAR(50) PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            phone VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Discs (
            name VARCHAR(250) NOT NULL,
            band VARCHAR(50) NOT NULL,
            PRIMARY KEY (name, band)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Bands (
            name VARCHAR(50) PRIMARY KEY,
            summary TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_has_discs (
            username VARCHAR(50),
            name_of_discs VARCHAR(50),
            CONSTRAINT pk_user_has_discs PRIMARY KEY (username, name_of_discs)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_likes_band (
            username VARCHAR(50),
            band_name VARCHAR(50),
            CONSTRAINT pk_user_likes_band PRIMARY KEY (username, band_name)
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

    # Open the SQL file
    with open('MOCK_DATA.sql', 'r') as file:
        sql = file.read()

    # Execute the SQL script
    cursor.execute(sql)

    # Commit the changes to the database
    conn.commit()

    # Execute the ALTER TABLE command to add foreign key constraint
    cursor.execute(foreign_keys)

    # Commit the changes to the database
    conn.commit()

    print("Data inserted successfully.")
    print("Foreign key added successfully.")


except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection objects
    cursor.close()
    conn.close()