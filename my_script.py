import os
import subprocess

password = os.environ.get('DB_PASSWORD')

def create_db():
    cmd = ["createdb", "-h", "localhost", "-p", "5432", "-U", "chryka", "music_band"]
    subprocess.run(cmd, check=True, env={"PGPASSWORD": password})

def drop_db():
    cmd = ["dropdb", "-h", "localhost", "-p", "5432", "-U", "chryka", "--if-exists", "music_band"]
    subprocess.run(cmd, check=True, env={"PGPASSWORD": password})

if __name__ == "__main__":
    drop_db()
    create_db()
    # do some work with the database...
   
