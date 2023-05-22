import os
from flask import Flask, jsonify, request
import psycopg2
from dotenv import load_dotenv
from Crypto.Cipher import AES
import recommend
import stats

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
    # Fetch the hashed password from the database based on the username
    # You need to implement this database query using the 'connection' object
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
    return jsonify({'discs': discs_list}), 200

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
        return jsonify({'message': "Requested disc not found."}), 404
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
    if not r:
        return jsonify({'message': "Requested disc not found."}), 404
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
        return jsonify({'message': "Requested disc not found."}), 404
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
        return jsonify({'message': "Requested disc not found."}), 404
    return jsonify({'most_listener_country': r}), 200

@app.route('/stats/countries/mostmusic', methods=['GET'])
def most_music_countries():
    # authentication -----
    auth = authenticate()
    if auth[1] != 200:
        return auth[0]
    # ------
    r = stats.countries_most_music(conn)
    if not r:
        return jsonify({'message': "Requested disc not found."}), 404
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
    if not r:
        return jsonify({'message': "Requested disc not found."}), 404
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
        return jsonify({'message': "Requested disc not found."}), 404
    return jsonify({'average_user_age': r}), 200


if __name__ == '__main__':
    app.run()
