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
            name VARCHAR(50) PRIMARY KEY,
            creation_date DATE NOT NULL,
            price FLOAT NOT NULL,
            band VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Bands (
            name VARCHAR(50) PRIMARY KEY,
            creation_date DATE,
            summary VARCHAR(10000)
        )
        """
    )

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

    print("Data inserted successfully.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection objects
    cursor.close()
    conn.close()