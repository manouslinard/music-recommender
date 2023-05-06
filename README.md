# Prerequisites

## API Key:
To load the data from lastfm api, you should put your api key in the config.py.

---
## Python Venv:
You should initialize the python venv in the same folder as requirements.txt (initial repo folder).<br>To create a python venv, run:
```
python3 -m venv env
```
Then to activate it:
```
source env/bin/activate
```
Once you activated the venv, you can install the requirements of the app like so (from project directory):
```
pip install -r puddle/requirements.txt
```
If you want to delete the venv, run:
```
sudo rm -rf env
```
---
## Psql Local Config:
To create required db, run:
```
createdb -h localhost -p 5432 -U postgres music_band
```
It is also important to set the postgres user's password to 'pass123' for django app to work.<br>
To delete created db, run:
```
dropdb -h localhost -p 5432 -U postgres music_band
```

## RUN Dockefile
docker build -t my_project .
docker run --rm -it --network="host" -e DB_PASSWORD=your_password my_project