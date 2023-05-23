import psycopg2
import os
from dotenv import load_dotenv

def find_user_friends(username, conn):
    with conn.cursor() as cur:
        query = """
            SELECT friend_username
            FROM user_friends
            WHERE username = %s
            UNION
            SELECT username
            FROM user_friends
            WHERE friend_username = %s
        """
        cur.execute(query, (username, username))
        results = cur.fetchall()
    merged_list = list(set([item[0] for item in results]))
    return merged_list

def get_user_discs(username, conn):
    with conn.cursor() as cur:
        query = """
            SELECT user_has_discs.disc_name, discs.band
            FROM user_has_discs
            JOIN discs ON user_has_discs.disc_name = discs.name
            WHERE user_has_discs.username = %s
        """

        cur.execute(query, (username,))
        data = cur.fetchall()
    return data


# print(get_user_discs("krawson0"))

def recommend_disc_user(username, conn):
    load_dotenv()
    n = int(os.getenv("NUMBER_REC_DISCS", 5))
    user_friends = find_user_friends(username, conn)

    user_discs = get_user_discs(username, conn)

    disc_frequencies = {}

    # Iterate over each friend and their discs
    for f in user_friends:
        friend_discs = get_user_discs(f, conn)

        # Filter out the discs that the user already has
        friend_discs = [disc for disc in friend_discs if disc not in user_discs]

        # Calculate the frequency of each remaining disc among friends
        for disc in friend_discs:
            if disc not in disc_frequencies:
                disc_frequencies[disc] = 1
            else:
                disc_frequencies[disc] += 1

    # Sort the discs based on their frequency in descending order
    recommended_discs = sorted(disc_frequencies.items(), key=lambda x: x[1], reverse=True)
    # n = min(n, len(recommended_discs))
    # Recommend the top n discs to the user
    return [disc for disc, _ in recommended_discs[:n]]


def recommend_all_user_discs(conn):
    print("Recommending User discs (according to friends network)...")
    with conn.cursor() as cur:
        query = """
            SELECT username
            FROM users
        """
        cur.execute(query)
        data = cur.fetchall()
        all_users = [item[0] for item in data]
        insert_query = "INSERT INTO user_rec_discs (username, disc_name, disc_band) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
        for u in all_users:
            for r in recommend_disc_user(u, conn):
                cur.execute(insert_query, (u, r[0], r[1]))
        conn.commit()
        print("Discs recommended to User.")


# print(recommend_disc_user("krawson0"))
if __name__ == "__main__":
    load_dotenv()
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=os.getenv("PSQL_HOST"),
        database=os.getenv("PSQL_DATABASE"),
        user=os.getenv("PSQL_USERNAME"),
        password=os.getenv("PSQL_PASSWORD")
    )
    recommend_all_user_discs(conn)
