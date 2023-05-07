import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# DELETE WHEN DONE ================================
load_dotenv()
# Connect to PostgreSQL database
conn = psycopg2.connect(
    host=os.getenv("PSQL_HOST"),
    database=os.getenv("PSQL_DATABASE"),
    user=os.getenv("PSQL_USERNAME"),
    password=os.getenv("PSQL_PASSWORD")
)
# =================================================

# Q1
def avg_user_band_age(conn, band_name):
    """
    Finds average age number of users that listen to an input band.
    """
    sql = """
    SELECT AVG(age) as avg_age
    FROM Users
    JOIN user_likes_band ON Users.username = user_likes_band.username
    JOIN Bands ON user_likes_band.band_name = Bands.name
    WHERE LOWER(Bands.name) = LOWER(%s);
    """

    # Execute the query and fetch the result
    with conn.cursor() as cur:
        cur.execute(sql, (band_name,))
        result = cur.fetchone()

    # Return the average age
    return round(result[0], 2)

# Q2
def countries_most_music(conn):
    """
    Returns a dictionary with countries and the users that each country has in the app.
    """
    cur = conn.cursor()
    query = """
    SELECT country, COUNT(*) FROM Users WHERE country != 'unregistered' GROUP BY country;
    """
    cur.execute(query)
    result = cur.fetchall()
    cur.close()

    country_dict = {}
    for row in result:
        country_dict[row[0]] = row[1]

    return country_dict

#Q3
def band_most_listeners(conn, band_name):
    """
    Returns the country with the most users who listen to the given band.
    """
    cur = conn.cursor()
    query = """
        SELECT country, COUNT(*) AS user_count
        FROM users
        JOIN user_likes_band ON users.username = user_likes_band.username
        WHERE LOWER(band_name) = LOWER(%s) AND country != 'unregistered'
        GROUP BY country
        ORDER BY user_count DESC
        LIMIT 1
    """
    cur.execute(query, (band_name,))
    result = cur.fetchone()
    cur.close()
    return result[0]

# Q4
def band_most_gender(conn, band_name):
    """
    Returns the gender that the most users who listen to a specific band are.
    """
    query = """
    SELECT gender
    FROM Users
    JOIN user_likes_band ON Users.username = user_likes_band.username
    JOIN Bands ON user_likes_band.band_name = Bands.name
    WHERE LOWER(Bands.name) = LOWER(%s)
    """
    cur = conn.cursor()
    cur.execute(query, (band_name,))
    rows = cur.fetchall()

    gender_count = {}
    for row in rows:
        if row[0] not in gender_count:
            gender_count[row[0]] = 1
        else:
            gender_count[row[0]] += 1

    max_gender = max(gender_count, key=gender_count.get)
    return max_gender

# Q5
def band_with_most_listeners(conn):
    """
    Returns the name of the band with the most listeners.
    """
    query = """
    SELECT band_name, COUNT(*) AS listeners_count
    FROM user_likes_band
    GROUP BY band_name
    ORDER BY listeners_count DESC
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        if result:
            return result[0]

# Q6
def avg_disc_count(conn):
    """
    Finds the average number of discs that users have.
    """
    query = """
        SELECT AVG(disc_count) AS avg_disc_count
        FROM (
            SELECT username, COUNT(*) AS disc_count
            FROM user_has_discs
            GROUP BY username
        ) AS user_disc_counts;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        return round(result[0])

# Q7
def band_users_by_country(conn, band_name):
    """
    Returns a dictionary with the countries and the users that listen to a given band.
    """
    query = """
        SELECT country, COUNT(*) AS count
        FROM Users
        JOIN user_likes_band ON Users.username = user_likes_band.username
        JOIN Bands ON user_likes_band.band_name = Bands.name
        WHERE LOWER(Bands.name) = LOWER(%s)
        GROUP BY country
    """
    with conn.cursor() as cur:
        cur.execute(query, (band_name,))
        rows = cur.fetchall()
        result = {row[0]: row[1] for row in rows}
    return result

# Q8
def disc_users_country(conn, disc_name):
    """
    Returns a list of countries of the users who have the specified disc.
    """
    query = """
    SELECT country
    FROM Users
    JOIN user_has_discs ON Users.username = user_has_discs.username
    JOIN Discs ON user_has_discs.disc_name = Discs.name AND user_has_discs.disc_band = Discs.band
    WHERE LOWER(Discs.name) = LOWER(%s);
    """

    with conn.cursor() as cur:
        cur.execute(query, (disc_name,))
        rows = cur.fetchall()
        return [row[0] for row in rows]

# Q9
def num_users_with_disc(conn, disc_name):
    """
    Returns the number of users that have a given disc.
    """
    query = """
    SELECT COUNT(DISTINCT username)
    FROM user_has_discs
    WHERE LOWER(disc_name) = LOWER(%s)
    """
    with conn.cursor() as cur:
        cur.execute(query, (disc_name,))
        result = cur.fetchone()
    return result[0]

# Q10
def disc_most_gender(conn, disc_name):
    """
    Finds the gender that the most users who have the given disc belong to.
    """
    sql = """
    SELECT gender, COUNT(*) as count
    FROM Users
    JOIN user_has_discs ON Users.username = user_has_discs.username
    WHERE LOWER(disc_name) = LOWER(%s)
    GROUP BY gender
    ORDER BY count DESC
    LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(sql, (disc_name,))
        result = cur.fetchone()

    if result is not None:
        return result[0]
    else:
        return None

# Q11
def most_listened_bands_by_country(conn):
    """
    Returns a dictionary with the countries as keys and as values a tuple with the most listened band by users in that 
    country and the number of users who have listened to that band.
    """

    query = """
        SELECT Users.country, Bands.name, COUNT(*) as listens
        FROM Users JOIN user_has_discs ON Users.username = user_has_discs.username
        JOIN Discs ON Discs.name = user_has_discs.disc_name AND Discs.band = user_has_discs.disc_band
        JOIN Bands ON Bands.name = Discs.band
        GROUP BY Users.country, Bands.name
        ORDER BY Users.country, listens DESC;
        """
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()

    most_listened = {}
    for country, band, listens in results:
        if country not in most_listened:
            most_listened[country] = (band, listens)
        elif listens > most_listened[country][1]:
            most_listened[country] = (band, listens)

    return {country: data for country, data in most_listened.items() if data[0] != 'unregistered'}

print(avg_user_band_age(conn, "scorpions"))
print(countries_most_music(conn))
print(band_most_listeners(conn, "scorpions"))
print(band_most_gender(conn, "scorpions"))
print(band_with_most_listeners(conn))
print(avg_disc_count(conn))
print(band_users_by_country(conn, "scorpions"))
print(disc_users_country(conn, "Anthology 3"))
print(num_users_with_disc(conn, "Anthology 3"))
print(disc_most_gender(conn, "Anthology 3"))
print(most_listened_bands_by_country(conn))