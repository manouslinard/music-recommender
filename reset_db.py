import os
import subprocess
from dotenv import load_dotenv
import load_api

load_dotenv()

password = os.getenv("PSQL_PASSWORD")
host=os.getenv("PSQL_HOST")
database=os.getenv("PSQL_DATABASE")
user=os.getenv("PSQL_USERNAME")
port=os.getenv("PSQL_PORT")

load_data = bool(int(os.environ.get('LOAD_DATA', 0)))

def create_db():
    cmd = ["createdb", "-h", host, "-p", port, "-U", user, database]
    subprocess.run(cmd, check=True, env={"PGPASSWORD": password})

def drop_db():
    cmd = ["dropdb", "-h", host, "-p", port, "-U", user, "--if-exists", database]
    subprocess.run(cmd, check=True, env={"PGPASSWORD": password})

if __name__ == "__main__":
    drop_db()
    create_db()
    print("Database restarted.")
    if load_data:
        print("Loading data to db...")
        load_api.load_api()
    else:
        print("Data not loaded to db.")
    # do some work with the database...
