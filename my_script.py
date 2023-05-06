import os
import subprocess
import config
password = os.environ.get('DB_PASSWORD')

def create_db():
    cmd = ["createdb", "-h", config.HOST, "-p", config.PORT, "-U", config.USERNAME, config.DATABASE]
    subprocess.run(cmd, check=True, env={"PGPASSWORD": password})

def drop_db():
    cmd = ["dropdb", "-h", config.HOST, "-p", config.PORT, "-U", config.USERNAME, "--if-exists", config.DATABASE]
    subprocess.run(cmd, check=True, env={"PGPASSWORD": password})

if __name__ == "__main__":
    drop_db()
    create_db()
    # do some work with the database...
   
