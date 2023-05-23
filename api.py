import os
from flask import Flask, jsonify, request
import psycopg2
from dotenv import load_dotenv
from Crypto.Cipher import AES
import recommend
import stats
import pandas as pd


app = Flask(__name__)

load_dotenv()
# Connect to PostgreSQL database
conn = psycopg2.connect(
    host=os.getenv("PSQL_HOST"),
    database=os.getenv("PSQL_DATABASE"),
    user=os.getenv("PSQL_USERNAME"),
    password=os.getenv("PSQL_PASSWORD")
)

# Database query to check if the username and password match
def check_credentials(username, password):
    with conn.cursor() as cur:
        query = "SELECT password FROM users WHERE username=%s"
        cur.execute(query, (username,))
        r = cur.fetchone()
        if not r:
            return False
        hashed_password = r[0][2:] # removes the prefix '\x81' from the hashed password

    # Secret key (pw)
    key = os.getenv('SECRET_KEY', '1234567890123456').encode('utf-8')
    # Convert hexadecimal string to bytes
    hashed_password_bytes = bytes.fromhex(hashed_password)
    # Decrypt the bytes
    cipher = AES.new(key)
    decrypted_bytes = cipher.decrypt(hashed_password_bytes)
    # Decode the decrypted bytes to string
    decrypted_string = decrypted_bytes.decode().replace(" ","")
    return password == decrypted_string

def authenticate():
    auth_fail_msg = 'Authentication failed'
    auth = request.authorization
    if not auth:
        return jsonify({'message': auth_fail_msg}), 401
    # Get the provided username and password from the request
    username = auth.username
    password = auth.password

    # Check if the provided credentials are valid
    if not username or not password or not auth or not check_credentials(username, password):
        # Return a 401 Unauthorized response if the credentials are invalid
        return jsonify({'message': auth_fail_msg}), 401

    return username, 200

def get_users_bands(username, conn):
    with conn.cursor() as cur:
        query = """
            SELECT Bands.name, Bands.summary
            FROM user_likes_band
            JOIN Users ON user_likes_band.username = Users.username
            JOIN Bands ON user_likes_band.band_name = Bands.name
            WHERE user_likes_band.username = %s
        """

        cur.execute(query, (username,))
        data = cur.fetchall()
    return data


def find_user_friends_detail(username, conn):
    query = """
        SELECT u.username, u.first_name, u.last_name, u.country, u.gender, u.age
        FROM user_friends uf
        JOIN Users u ON uf.friend_username = u.username
        WHERE uf.username = %s
        UNION
        SELECT u.username, u.first_name, u.last_name, u.country, u.gender, u.age
        FROM user_friends uf
        JOIN Users u ON uf.username = u.username
        WHERE uf.friend_username = %s
    """
    params = (username, username)
    df = pd.read_sql(query, conn, params=params)
    df['age'] = df['age'].replace(-1, 'unregistered')
    df['gender'] = df['gender'].replace('N', 'unregistered')
    users = df.to_dict('records')
    return users

def find_specific_friends(username, friend_name, conn):
    friends = find_user_friends_detail(username, conn)
    for f in friends:
        if friend_name == f["username"]:
            return f
    return None


@app.route('/discs', methods=['GET'])
def get_user_discs():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    discs = recommend.get_user_discs(username, conn)
    # Create a list of dictionaries with keys 'disc_name' and 'band_name'
    discs_list = [{'disc_name': disc[0], 'band': disc[1]} for disc in discs]

    # Return the user's discs as JSON response
    return jsonify({'user_discs': discs_list}), 200

@app.route('/bands', methods=['GET'])
def get_user_bands():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    bands = get_users_bands(username, conn)
    bands_list = [{'band_name': band[0], 'summary': band[1]} for band in bands]
    return jsonify({'user_bands': bands_list}), 200

@app.route('/recommend', methods=['GET'])
def get_user_recommend():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    recommend_list = recommend.recommend_disc_user(username,conn)
    discs_list = [{'disc_name': disc[0], 'band': disc[1]} for disc in recommend_list]
    return jsonify({'recommended': discs_list}), 200

@app.route('/friends', methods=['GET'])
def get_user_friends():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    friends_list = find_user_friends_detail(username,conn)
    return jsonify({'friends': friends_list}), 200

@app.route('/friends/<string:friends_username>', methods=['GET'])
def get_requested_friend(friends_username):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    friend = find_specific_friends(friends_username, username, conn)
    if not friend:
        return jsonify({'message': "Requested friend not found."}), 404
    return jsonify({'friend': friend}), 200

@app.route('/discs/friends', methods=['GET'])
def get_user_friends_discs():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    friends_list = recommend.find_user_friends(username,conn)
    friends_discs = []
    for friend in friends_list:
        discs = recommend.get_user_discs(friend, conn)
        discs_list = [{'disc_name': disc[0], 'band': disc[1]} for disc in discs]
        friends_discs.append({"username":friend, "discs":discs_list})

    return jsonify({'friends_discs': friends_discs}), 200

@app.route('/discs/friends/<string:friends_username>', methods=['GET'])
def get_requested_friend_discs(friends_username):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    friend = find_specific_friends(friends_username,username,conn)
    if not friend:
        return jsonify({'message': "Requested friend not found."}), 404
    friend_discs = []
    discs = recommend.get_user_discs(friend["username"], conn)
    discs_list = [{'disc_name': disc[0], 'band': disc[1]} for disc in discs]
    friend_discs.append({"username":friend["username"], "discs":discs_list})
    return jsonify({'friend_discs':friend_discs}), 200

@app.route('/bands/friends', methods=['GET'])
def get_user_friends_bands():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    friends_list = recommend.find_user_friends(username,conn)
    friends_bands = []
    for friend in friends_list:
        friend_bands = (friend,get_users_bands(friend, conn))
        friends_bands.extend(friend_bands)

    return jsonify({'discs': friends_bands}), 200

@app.route('/bands/friends/<string:friends_username>', methods=['GET'])
def get_requested_friend_bands(friends_username):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    username = auth[0]
    # ------
    check_friends = find_specific_friends(friends_username,username,conn)
    if not check_friends:
        return jsonify({'message': "Requested friend not found."}), 404
    friend_discs = get_users_bands(friends_username, conn)
    return jsonify({'discs':friend_discs}), 200

@app.route('/price/<string:disc_name>/history', methods=['GET'])
def get_web_scrape_disc_price(disc_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    with conn.cursor() as cur:
        query = """
        SELECT values from disc_prices where name = %s
        """
        cur.execute(query, (disc_name,))
        prices = cur.fetchall()
    return jsonify({'prices':prices}), 200

@app.route('/info/discs/<string:disc_name>', methods=['GET'])
def get_disc_info_last_price(disc_name):
    with conn.cursor() as cur:
        query = """
        SELECT dp.values, b.summary
        FROM disc_prices dp
        JOIN Discs d ON dp.name = d.name
        JOIN Bands b ON d.band = b.name
        WHERE dp.name = %s AND d.name = %s
        ORDER BY dp.date DESC
        LIMIT 1;

        """
        cur.execute(query, (disc_name, disc_name))
        info = cur.fetchall()
    return jsonify({'disc info': info}), 200

@app.route('/info/bands/<string:band_name>', methods=['GET'])
def get_band_info(band_name):
    with conn.cursor() as cur:
        query = """
        SELECT name,summary from Bands where name = %s
        """
        cur.execute(query, (band_name,))
        info = cur.fetchall()
    return jsonify({'disc info': info}), 200



@app.route('/stats/topdiscs/<int:disc_num>', methods=['GET'])
def get_topn_discs(disc_num):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    l = stats.top_x_discs_by_quantity(conn, disc_num)
    discs_list = [{'disc_name': disc[0], 'band': disc[1], 'quantity': disc[2]} for disc in l]

    return jsonify({'discs': discs_list}), 200

@app.route('/stats/bands/mostbands', methods=['GET'])
def get_mostbands_countries():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    l = stats.most_listened_bands_by_country(conn)
    bands_list = [{'country': b, 'band': l[b][0], 'quantity': l[b][1]} for b in l]
    return jsonify({'top_bands': bands_list}), 200

@app.route('/stats/discs/mostgender/<string:disc_name>', methods=['GET'])
def disc_most_gender(disc_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    disc_name = disc_name.replace("-"," ")
    # print(disc_name)
    r = stats.disc_most_gender(conn, disc_name)
    if r is None:
        return jsonify({'message': "Requested disc not found."}), 404
    return jsonify({'most_gender': r}), 200

@app.route('/stats/discs/usercount/<string:disc_name>', methods=['GET'])
def disc_user_count(disc_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    disc_name = disc_name.replace("-"," ")
    # print(disc_name)
    r = stats.num_users_with_disc(conn, disc_name)
    if not r:
        return jsonify({'message': "Requested disc not found."}), 404
    return jsonify({'number_of_users': r}), 200

@app.route('/stats/discs/heritage/<string:disc_name>', methods=['GET'])
def disc_user_country(disc_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    disc_name = disc_name.replace("-"," ")
    r = stats.disc_users_country(conn, disc_name)
    if not r:
        return jsonify({'message': "Requested disc not found."}), 404
    return jsonify({'country': r}), 200

@app.route('/stats/bands/heritage/<string:band_name>', methods=['GET'])
def band_user_country(band_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    band_name = band_name.replace("-"," ")
    r = stats.band_users_by_country(conn, band_name)
    # print(r)
    # TODO: fix band not exist in stats.py
    if not r:
        return jsonify({'message': "Requested band not found."}), 404
    bands_list = [{'country': b, 'user_number': r[b]} for b in r]
    return jsonify({'countries': bands_list}), 200

@app.route('/stats/mostband', methods=['GET'])
def most_band():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    r = stats.band_with_most_listeners(conn)
    return jsonify({'most_listened_band': r}), 200

@app.route('/stats/mostgender/<string:band_name>', methods=['GET'])
def band_most_gender(band_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    band_name = band_name.replace("-"," ")
    r = stats.band_most_gender(conn, band_name)
    # TODO: return None if band does not exist in stats.py
    if not r:
        return jsonify({'message': "Requested band not found."}), 404
    return jsonify({'most_gender': r}), 200

@app.route('/stats/bandcountry/<string:band_name>', methods=['GET'])
def band_most_user_heritage(band_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    band_name = band_name.replace("-"," ")
    r = stats.band_most_listeners(conn, band_name)
    # TODO: fix when input non existing band name in stats.py.
    if not r:
        return jsonify({'message': "Requested band not found."}), 404
    return jsonify({'most_listener_country': r}), 200

@app.route('/stats/countries/mostmusic', methods=['GET'])
def most_music_countries():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    r = stats.countries_most_music(conn)
    user_list = [{'country': b, 'user_number': r[b]} for b in r]
    return jsonify({'users': user_list}), 200


@app.route('/stats/average/discs', methods=['GET'])
def average_disc():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    r = stats.avg_disc_count(conn)
    return jsonify({'avg_user_disc': r}), 200

@app.route('/stats/average/age/<string:band_name>', methods=['GET'])
def average_user_band_age(band_name):
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    band_name = band_name.replace("-"," ")
    r = stats.avg_user_band_age(conn, band_name)
    # TODO: fix when input non existing band name in stats.py.
    if not r:
        return jsonify({'message': "Requested band not found."}), 404
    return jsonify({'average_user_age': r}), 200


if __name__ == '__main__':
    app.run()
