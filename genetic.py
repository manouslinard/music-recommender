from random import choices, randint, randrange, random, sample
import os
from dotenv import load_dotenv
import pandas as pd
import warnings
import psycopg2

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# first_example = [
#     {"name": 'disc1', "want": 3, "price": 2200},
#     {"name": 'disc2', "want": 2, "price": 160},
#     {"name": 'disc3', "want": 5, "price": 350},
#     {"name": 'disc4', "want": 4, "price": 333},
#     {"name": 'disc5', "want": 1, "price": 192},
# ]

def generate_genome(length: int) -> list:
    # generates a genome - binary string of random 1s and 0s.
    return choices([0, 1], k=length)

def generate_population(size: int, genome_length: int) -> list:
    return [generate_genome(genome_length) for _ in range(size)] # generates a list of genomes.

def fitness(genome: list, things: list, price_limit: int) -> int:
    if len(genome) != len(things):
        raise ValueError("genome and things must be of same length")
    price = 0
    want = 0
    for i, thing in enumerate(things):
        if genome[i] == 1:
            # adds only the 1s values and price.
            price += thing["price"]
            want += thing["want"]

            # if the price is exceeded, return 0.
            if price > price_limit:
                return 0
    # print(genome, want)
    return want

def selection_pair(population, things, price_limit) -> list:
    # returns the pair of the generation -> we import choices because it is not necessarily the most strong pair (but is higly likely with the weights)
    weights = [fitness(genome, things, price_limit) for genome in population]
    if all(weight == 0 for weight in weights):
        return sample(population, k=2)
    return choices(
        population=population,
        weights=weights,
        k=2 # gets a pair
    )

def crossover(parent_a: list, parent_b: list) -> list:
    # mating of the parents and creation of 2 children
    if len(parent_a) != len(parent_b):
        raise ValueError("Genomes a and b must be the same lenght.")
    length = len(parent_a)
    if length < 2:
        return parent_a, parent_b
    p = randint(1, length - 1)
    return parent_a[0:p] + parent_b[p:], parent_b[0:p] + parent_a[p:]   # returns a pair from first part of a and last part of b and the opposite.


def mutation(genome: list, num: int = 1, probability: float = 0.5):
    # mutation (of the children) to create new gens!
    for _ in range(num):
        index = randrange(len(genome))
        genome[index] = genome[index] if random() > probability else abs(genome[index] - 1) # changes a genome index with a probability of 0.5
    return genome


def run_evolution(pop_size, genome_length, things, price_limit, generation_limit) -> list:

    # create the population
    population = generate_population(pop_size, genome_length)
    # itterate throuth the number of generations:
    for i in range(generation_limit):
        population = sorted(
            population,
            key=lambda genome: fitness(genome, things, price_limit),
            reverse=True
        )

        next_generation = population[0:2]   # keeps the top 2 solutions for next gen (elytism).

        for j in range(int(len(population) / 2) - 1):   # does this for all the number of remaining pairs:
            # print(population, things, price_limit)
            parents = selection_pair(population, things, price_limit)
            child_a, child_b = crossover(parents[0], parents[1])
            child_a = mutation(child_a)
            child_b = mutation(child_b)
            next_generation += [child_a, child_b]

        population = next_generation

    # returns the best population (of next generations) + the i generation we are at.
    population = sorted(
        population,
        key=lambda genome: fitness(genome, things, price_limit),
        reverse=True
    )
    # return population, i
    return population[0]    # returns the best genome.

def genetic_knapshack(conn, pop_size, generation_limit):
    # the following query returns the username, user's money, the disc name and band (from those the user wants),
    # the wanted level of the disc (from 1 to 5) for this user and the latest price of a disc.
    query = """
        SELECT ud.username, users.money, ud.disc_name, ud.disc_band, ud.want, dp.values
        FROM user_wants_discs ud 
        JOIN (
            SELECT name, band, MAX(date) AS max_date
            FROM disc_prices
            GROUP BY name, band
        ) max_dp ON ud.disc_name = max_dp.name AND ud.disc_band = max_dp.band
        JOIN disc_prices dp ON ud.disc_name = dp.name AND ud.disc_band = dp.band AND max_dp.max_date = dp.date
        JOIN users ON users.username = ud.username;
    """
    df = pd.read_sql_query(query, conn)
    df.rename(columns={'values': 'price'}, inplace=True)

    user_wants = []
    # Extract unique usernames and corresponding money
    user_money = df[['username', 'money']].drop_duplicates().reset_index(drop=True)
    for _, row in user_money.iterrows():
        username = row['username']
        filtered_df = df.loc[df['username'] == username]
        wanted = []
        for _, disc in filtered_df.iterrows():
            wanted.append({"name": disc["disc_name"], "band":disc["disc_band"], "want":disc["want"], "price":disc["price"]})
        d = {"username": username, "money": row["money"], "wanted": wanted}
        user_wants.append(d)

    res = []
    for u in user_wants:
        sum_p = sum(item['price'] for item in u['wanted'])

        if sum_p <= u["money"]: # if user can afford all wanted discs -> buys them all.
            discs = [(item["name"], item["band"]) for item in u['wanted']]
            d = {"username": u["username"], "discs": discs}
            res.append(d)
            continue

        min_price = min(u["wanted"], key=lambda x: x['price'])['price']
        if min_price < u["money"]:
            # print(min_price, u["money"])
            l = run_evolution(pop_size, len(u["wanted"]), u["wanted"], u["money"], generation_limit)
            w_d = []
            for i, w in enumerate(l):
                if w == 1:
                    w_d.append((u["wanted"][i]["name"], u["wanted"][i]["band"]))
            res.append({"username":u["username"], "discs":w_d})
        else:
            res.append({"username":u["username"], "discs":[]})
        # print(pop_size, len(u["wanted"]), u["wanted"], u["money"], generation_limit)       
    return res

def load_db_wanted_knapsack(conn, population_size, generation_limit):
    print("Recommending Discs with (Genetic) Knapsack...")
    q = """
    CREATE TABLE IF NOT EXISTS user_rec_discs_knapsack (
        username VARCHAR(50),
        disc_name VARCHAR(250),
        disc_band VARCHAR(50),
        CONSTRAINT pk_user_rec_discs_knp PRIMARY KEY (username, disc_name, disc_band),
        CONSTRAINT fk_user_rec_discs_username_knp FOREIGN KEY (username)
            REFERENCES users (username),
        CONSTRAINT fk_user_rec_discs_disc_knp FOREIGN KEY (disc_name, disc_band)
            REFERENCES discs (name, band)
    )
    """
    cursor = conn.cursor()
    cursor.execute(q)
    conn.commit()

    r = genetic_knapshack(conn, population_size, generation_limit)
    # print(r)
    print("Filling Knapsack Table...")

    for user in r:
        discs = user["discs"]
        if discs:
            for d in discs:
                insert_query = "INSERT INTO user_rec_discs_knapsack VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                cursor.execute(insert_query, (user["username"], d[0], d[1]))
    conn.commit()
    print("Discs Recommended With Knapsack.")

if __name__ == "__main__":
    load_dotenv()
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=os.getenv("PSQL_HOST"),
        database=os.getenv("PSQL_DATABASE"),
        user=os.getenv("PSQL_USERNAME"),
        password=os.getenv("PSQL_PASSWORD")
    )
    population_size = int(os.environ.get('POPULATION_SIZE', 5))
    gen_limit = int(os.environ.get('GENERATION_LIMIT', 10))
    load_db_wanted_knapsack(conn, population_size, gen_limit)
    # Query to get the results in the database:
    # SELECT u.username, u.money, d.name AS disc_name, dp.values AS disc_price
    # FROM users u
    # JOIN user_rec_discs_knapsack urd ON u.username = urd.username
    # JOIN (
    #     SELECT dp.name, dp.band, MAX(dp.date) AS max_date
    #     FROM disc_prices dp
    #     GROUP BY dp.name, dp.band
    # ) max_dp ON urd.disc_name = max_dp.name AND urd.disc_band = max_dp.band
    # JOIN discs d ON max_dp.name = d.name AND max_dp.band = d.band
    # JOIN disc_prices dp ON max_dp.name = dp.name AND max_dp.band = dp.band AND max_dp.max_date = dp.date
    # ;
