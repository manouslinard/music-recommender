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
pip install -r requirements.txt
```
If you want to delete the venv, run:
```
sudo rm -rf env
```
---
## Env File:
You should create a .env file in the projects directory, which will contain the following:
```
BAND_NAMES=coldplay scorpions the+beatles queen acdc u2
PSQL_PASSWORD=pass123
PSQL_USERNAME=postgres
PSQL_HOST=localhost
PSQL_PORT=5432
PSQL_DATABASE=music_band
YOUR_API_KEY=YOUR_API_KEY
LOAD_DATA=1
LOAD_PRICES=0
DISCOGS_KEY=YOUR_DISCOGS_KEY
DISCOGS_SECRET=YOUR_DISCOGS_SECRET
WEB_SCRAPE_PRICES=1
MAX_DISC_SCRAPE=-1
NUMBER_REC_DISCS=1
SECRET_KEY=YOUR_SECRET_KEY
POPULATION_SIZE=6
GENERATION_LIMIT=11
```
The secret key is a sting used for user password encryption. This should have exact length 16, 24 or 32 characters. Do not change it when saving the users to database and then retrieving them in api.
<br>
In the BAND_NAMES variable, you declare the bands that you want to get data for (these are the bands saved in the db).
<br>
If you dont want to insert the ready-users from csv to db, set LOAD_DATA=0.
<br>
If you want to load the prices of csv file, set LOAD_PRICES=1 (it is recommended to do this with not a lot of bands, which are declared in the top of load_api.py).
<br>
If you want to load prices from webscraping discogs, both LOAD_DATA and WEB_SCRAPE_PRICES should be equal to 1. If also you want to have a max limit of discs scraped per band, set MAX_DISC_SCRAPE to any positive number you want (this positive number is also the max limit).
<br>
NUMBER_REC_DISCS is the number of discs that will be recommended to each user.
<br>
POPULATION_SIZE is the population size for the genetic algorithm used in knapsack problem.
GENERATION_LIMIT is the generation limit for the genetic algorithm.

---
## Running the code:
After you have configured the env file, you can run all the tasks by executing the following command:
```
python3 reset_db.py
```
<br>
To show the stats - graphs, run:

```
python3 stats.py
```

---
## Users.csv file:
In the users.csv file you can put the users of the project. Some users have also null attributes to test the handling of the NaN values. Users should always have a username.

---
## Psql Local Config:
To create required db, run:
```
createdb -h localhost -p 5432 -U postgres music_band
```
To delete created db, run:
```
dropdb -h localhost -p 5432 -U postgres music_band
```
For an easy restart of the database, just run:
```
python3 reset_db.py
```
This will drop and recreate the database tables.

---

## Web-Scraping:

The scrape.py scrapes prices from [discogs.com](https://www.discogs.com/). To get real prices, run:
```
python3 scrape.py
```
---
## Flask API:

To run the flask api, run (from root directory):
```
python3 api.py
```
The api runs in [localhost:5000](http://localhost:5000/). You can see more about the API endpoints in the documentation (see below).

---
## Documentation:
You can find the documentation of this project in Greek [here](/files/DecisionSystem_Assignment_GR.pdf). The documentation has also been translated automatically in English and can be found [here](/files/DecisionSystem_Assignment_ENG.pdf).

---
## Project Presentation:
You can view a powerpoint presentation about the web scraping and flask api used in the project [here (greek)](/files/Webscraper_Flask_API_GR.pdf). You can also find the presentation in English (using automatic translation) [here](/files/Webscraper_Flask_API_ENG.pdf).
