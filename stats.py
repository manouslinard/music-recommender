import psycopg2
import os
from dotenv import load_dotenv
from sklearn.metrics import mean_squared_error
from math import sqrt
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")


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

def ARIMA_train(series,discname,band):
    # prepare data
    X = series.values
    size = int(len(X) * 0.74)
    train, test = X[0:size], X[size:len(X)]

    persistence_values = range(1, 25)
    scores = []
    all_predictions = []
    actual_values = []
    rmsel = []
    for p in persistence_values:
        # walk-forward validation
        history = [x for x in train]
        predictions = list()
        for t in range(len(test)):
            # make prediction
            yhat = history[-p]
            predictions.append(yhat)
            # observation
            obs = test[t]
            history.append(obs)
            # report performance for current timestep only
            rmse = sqrt(mean_squared_error([obs], [yhat]))
            rmsel.append(rmse)
            scores.append(rmse)
            print('p=%d, t=%d, predicted=%f, expected=%f, RMSE:%.3f' % (p, t, yhat, obs, rmse))
        all_predictions.append(predictions)
        actual_values.append(test)

    return all_predictions, actual_values, persistence_values 

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
    query = """
    SELECT country, COUNT(*) FROM Users WHERE country != 'unregistered' GROUP BY country;
    """
    df = pd.read_sql(query, conn)
    country_dict = dict(zip(df['country'], df['count']))
    return country_dict

#Q3
def band_most_listeners(conn, band_name):
    """
    Returns the country with the most users who listen to the given band.
    """
    query = """
        SELECT country, COUNT(*) AS user_count
        FROM users
        JOIN user_likes_band ON users.username = user_likes_band.username
        WHERE LOWER(band_name) = LOWER(%s) AND country != 'unregistered'
        GROUP BY country
        ORDER BY user_count DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (band_name,))
        result = cur.fetchone()
    return result[0]

# Q4
def band_most_gender(conn, band_name):
    """
    Returns the gender that the most users who listen to a specific band are.
    """
    query = """
    SELECT *
    FROM Users
    JOIN user_likes_band ON LOWER(Users.username) = LOWER(user_likes_band.username)
    JOIN Bands ON LOWER(user_likes_band.band_name) = LOWER(Bands.name)
    """
    df = pd.read_sql(query, conn)
    max_gender = df.groupby('gender').size().idxmax()
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
    return result[0] if result else None

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
    # execute the query and read the results into a dataframe
    df = pd.read_sql(query, conn, params=[band_name])

    # convert the dataframe to a dictionary
    result = df.set_index('country')['count'].to_dict()
    result.pop('unregistered')  # removes unregistered
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
    # Use read_sql_query to execute the SQL query and load results into a pandas DataFrame
    df = pd.read_sql_query(query, conn, params=[disc_name])

    # Convert the DataFrame into a list and return
    return df['country'].tolist()

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
    return result[0] if result else None

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
    df = pd.read_sql(query, conn)
    df = df[df['name'] != 'unregistered']
    df = df.groupby('country').first()
    df = df[['name', 'listens']].apply(lambda x: (x[0], x[1]), axis=1)
    return df.to_dict()

# Q12
def top_x_discs_by_quantity(conn, x=5):
    """
    Returns a list of the top discs in quantity that the users have.
    """
    query = f"""
        SELECT disc_name, disc_band, COUNT(*) AS num_users
        FROM user_has_discs
        GROUP BY disc_name, disc_band
        ORDER BY num_users DESC
        LIMIT {x};
        """
    # Use pandas read_sql to execute the query and create a DataFrame from the results
    df = pd.read_sql(query, conn)

    # Create a list of tuples for the top x discs
    top_x_discs = df[['disc_name', 'disc_band', 'num_users']].to_records(index=False).tolist()

    return top_x_discs


# print(avg_user_band_age(conn, "scorpions"))
# print(countries_most_music(conn))
# print(band_most_listeners(conn, "coldplay"))
# print(band_most_gender(conn, "coldplay"))
# print(band_with_most_listeners(conn))
# print(avg_disc_count(conn))
# print(band_users_by_country(conn, "coldplay"))
# print(disc_users_country(conn, "Moses"))
# print(num_users_with_disc(conn, "Anthology 3"))
# print(disc_most_gender(conn, "Clocks"))
# print(most_listened_bands_by_country(conn))
# print(top_x_discs_by_quantity(conn))

# ==================================== PLOT =====================


def plot_avg_user_band_age(conn, band_names):
    """
    Plots the average age of users for a list of bands.
    """
    avg_ages = []
    band_names_copy = []
    for band_name in band_names:
        b_name = band_name.replace("+", " ")
        band_names_copy.append(b_name)
        avg_age = avg_user_band_age(conn, b_name)
        avg_ages.append(avg_age)

    # Create the bar plot
    fig, ax = plt.subplots()
    ax.bar(band_names_copy, avg_ages)
    ax.set_xlabel('Band Name')
    ax.set_ylabel('Average Age')
    ax.set_title('Average Age of Users by Band')
    plt.show()

def plot_countries_most_music(conn):
    """
    Plots a line chart with the number of users per country.
    """
    country_dict = countries_most_music(conn)
    countries = list(country_dict.keys())
    users = list(country_dict.values())

    plt.plot(countries, users, marker='o')
    plt.xticks(rotation=90)
    plt.xlabel("Country")
    plt.ylabel("Number of users")
    plt.title("Number of users per country")
    plt.show()

def plot_top_x_discs_by_quantity(conn, x=5):
    """
    Generates a pie chart of the top X discs in quantity that the users have.
    """
    top_x_discs = top_x_discs_by_quantity(conn, x)

    labels = [f"{disc_name} by {disc_band} ({num_users})" for disc_name, disc_band, num_users in top_x_discs]
    values = [num_users for disc_name, disc_band, num_users in top_x_discs]

    fig, ax = plt.subplots()
    ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)

    # Add title and legend
    ax.set_title(f'Top {x} Discs by Quantity')
    # ax.legend(labels, loc='upper right')

    plt.show()

def plot_disc_gender_distribution(conn, x=5):
    """
    Generates a bar chart showing the gender distribution of the users who have the top x discs by quantity.
    """
    top_discs = top_x_discs_by_quantity(conn, x)
    disc_names = [d[0] for d in top_discs]
    gender_counts = []
    for disc_name in disc_names:
        gender_counts.append((disc_name, disc_most_gender(conn, disc_name)))

    # Filter out any discs with no gender data
    gender_counts = [g for g in gender_counts if g[1] is not None]

    # Count the number of users for each gender
    gender_totals = {'M': 0, 'F': 0}
    for disc_name, gender in gender_counts:
        gender_totals[gender] += 1

    # Create the bar chart
    fig, ax = plt.subplots()
    ax.bar(gender_totals.keys(), gender_totals.values())
    ax.set_xlabel('Gender')
    ax.set_ylabel('Number of Users')
    ax.set_title(f'Gender Distribution of Users with Top {x} Discs by Quantity')

    plt.show()

def plot_users_by_gender(conn):
    """
    Plots a bar chart with the number of users per gender.
    """
    query = """
        SELECT gender, COUNT(*) AS num_users
        FROM Users
        GROUP BY gender;
        """
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()

    genders = ["Unknown" if row[0]=="N" else row[0] for row in results]
    num_users = [row[1] for row in results]

    plt.bar(genders, num_users)
    plt.xlabel("Gender")
    plt.ylabel("Number of users")
    plt.title("Number of users per gender")
    plt.show()

def plot_user_age(conn):
    """
    Plots a line chart with the age distribution of all users.
    """
    sql = "SELECT age FROM Users WHERE age IS NOT NULL;"
    with conn.cursor() as cur:
        cur.execute(sql)
        ages = [row[0] for row in cur.fetchall()]

    # Count the number of users in each age group
    age_counts = {}
    for age in ages:
        if age not in age_counts:
            age_counts[age] = 1
        else:
            age_counts[age] += 1

    # Sort the age groups by age
    age_groups = sorted(age_counts.keys())

    # Plot the line chart
    plt.plot(age_groups, [age_counts[age] for age in age_groups])
    plt.xlabel("Age")
    plt.ylabel("Number of users")
    plt.title("Age Distribution of Users")
    plt.show()

def plot_time_series(conn, discname, band):
    # generate a new time series for this disc
    sql = f"SELECT date,values FROM disc_prices as d WHERE d.band='{band}' and d.name='{discname}'"
    df = pd.read_sql_query(sql, conn)
    if df.empty:
        print(f"No results found for disc '{discname}' by band '{band}'.")
        return
    date_rng = pd.date_range(start=df['date'].iloc[0], end=df['date'].iloc[-1], freq='D')
    val = 40 + 15 * np.tile(np.sin(np.linspace(-np.pi, np.pi, len(date_rng))), 5)
    val_diff = val[len(date_rng)-1] - val[-1]
    val = np.append(val[:len(date_rng)], [val[-1] + val_diff] * (len(date_rng) - len(val))) + 5 * np.random.rand(len(date_rng))
    series = pd.DataFrame({'values': val}, index=pd.DatetimeIndex(date_rng))
    ax = series.plot()
    ax.set_title(f"{discname} by {band}")
    plt.show()

    # train the ARIMA model and get predictions and actual values
    all_predictions, actual_values, persistence_values = ARIMA_train(series, discname, band)

    # plot the actual vs. predicted values for the last persistence value
    plt.plot(actual_values[-1], label='actual')
    plt.plot(all_predictions[-1], label='predicted')
    plt.legend()
    plt.title(f"Actual vs. Predicted for {discname} by {band} (p={persistence_values[-1]})")
    plt.show()

# plot_avg_user_band_age(conn, band_names)
# plot_countries_most_music(conn)
# plot_top_x_discs_by_quantity(conn)
# plot_disc_gender_distribution(conn)
# plot_users_by_gender(conn)
# plot_user_age(conn)
plot_time_series(conn,"Live 2003","Coldplay")
