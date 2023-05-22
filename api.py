import os
from flask import Flask, jsonify, request
import psycopg2
from dotenv import load_dotenv
from Crypto.Cipher import AES
import recommend

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
        hashed_password = cur.fetchone()[0][2:] # removes the prefix '\x81' from the hashed password

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

@app.route('/discs', methods=['GET'])
def get_user_discs():
    # Get the provided username and password from the request
    auth = request.authorization
    username = auth.username
    password = auth.password

    # Check if the provided credentials are valid
    if not username or not password or not auth or not check_credentials(username, password):
        # Return a 401 Unauthorized response if the credentials are invalid
        return jsonify({'message': 'Authentication failed'}), 401

    discs = recommend.get_user_discs(username, conn)
    # Create a list of dictionaries with keys 'disc_name' and 'band_name'
    discs_list = [{'disc_name': disc[0], 'band': disc[1]} for disc in discs]

    # Return the user's discs as JSON response
    return jsonify({'discs': discs_list}), 200


if __name__ == '__main__':
    app.run()
