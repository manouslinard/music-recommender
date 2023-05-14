from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from scipy.stats.mstats import winsorize
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from webdriver_manager.firefox import GeckoDriverManager


def load_prices_discogs(artist_name, disc_name, MAX_PAGES=10, write_csv=False, plot=False):

    artist_name = str(artist_name)
    disc_name = disc_name.replace(' ', '-').replace('/', '-')
    disc_name = '-'.join(filter(None, disc_name.split('-')))

    # print(artist_name, disc_name)

    # create Chrome options (no tab):
    try:
        options = Options()
        options.add_argument('--headless')  # remove this if you want the browser to appear.
        # create Chrome service - installs chrome driver:
        service = Service(ChromeDriverManager().install())
        # initialise driver:
        driver = webdriver.Chrome(service=service, options=options)
    except: # starts firefox    
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')  # remove this if you want the browser to appear.
        service = webdriver.firefox.service.Service(executable_path=GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)# go to url:

    discogs_url = "https://www.discogs.com"

    page_url = discogs_url+"/artist/"+artist_name+"?limit=500"
    print(f"Searching for disc: {disc_name}")
    print('Opening Page...')
    driver.get(page_url)

    # wait for button to be present
    wait = WebDriverWait(driver, 10)
    button = wait.until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))

    # click the button
    button.click()
    time.sleep(5)

    table = driver.find_element(By.ID, 'artist')
    table_html = table.get_attribute('outerHTML')

    albums = {}

    soup = BeautifulSoup(table_html, 'html.parser')

    a_tags = soup.find_all('a', href=lambda href: href and ('/master/' in href or '/release/' in href))
    for a_tag in a_tags:
        disc_txt = a_tag.text
        disc_txt = disc_txt.replace(' ', '-').replace('/', '-')
        disc_txt = '-'.join(filter(None, disc_txt.split('-')))
        if disc_txt not in albums:    # keeps the top albums
            albums[disc_txt] = discogs_url+a_tag['href']

    # print(albums.keys())

    if disc_name not in albums:
        return pd.DataFrame()   # returns empty pandas Dataframe.

    driver.get(albums[disc_name])
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    buy_button = soup.find('a', string='Buy a copy')
    if buy_button:
        buy_link = buy_button.get('href')
        albums[disc_name] = discogs_url+buy_link+"&limit=250"
    else:
        print("Not for Sale.")
        #print(disc_name)
        return pd.DataFrame()   # returns empty pandas Dataframe.

    visited = []
    data = []

    for n in range(1, MAX_PAGES):
        print(f"Page number: {n}")
        driver.get(albums[disc_name]+f"&page={n}")
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        buy_url = []
        albums_buy_url = {} # has a list of all buy url and their details.
        buy_buttons = soup.find_all('a', string='View Release Page')
        if buy_buttons:
            for buy_button in buy_buttons:
                buy_link = discogs_url+buy_button.get('href')
                # print(buy_link)
                buy_url.append(buy_link)
                # albums[disc_name]["view_url"].append(discogs_url+buy_link)
            albums_buy_url[disc_name] = buy_url
        else:
            if n > 1:
                print(f"Reached max available page number ({n-1})")
            else:
                print("Disc not found.")
            break


        # print(albums_buy_url[disc_name])
        for buy_u in albums_buy_url[disc_name]:
            if buy_u in visited:
                continue
            visited.append(buy_u)
            driver.get(buy_u)
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            # response = requests.get(buy_url)
            # soup = BeautifulSoup(response.content, 'html.parser')

            div_tag = soup.find('div', class_='items_3gMeU')
            ul_tags = div_tag.find_all('ul')

            # # Extracting "Have" and "Want" counts
            # have_count = ul_tags[0].find('a').text
            # want_count = ul_tags[0].find_all('a')[1].text

            # # Extracting average rating and rating count
            # avg_rating = ul_tags[0].find_all('span')[1].text.strip()
            # rating_count = ul_tags[0].find_all('a')[2].text

            print(buy_u)
            # Extracting last sold date and price statistics
            try:
                last_sold = ul_tags[1].find('time').get('datetime')
                lowest_price = ul_tags[1].find_all('span')[2].text
                median_price = ul_tags[1].find_all('span')[4].text
                highest_price = ul_tags[1].find_all('span')[6].text
                data.append({"date": last_sold, "lowest_price": lowest_price, "median_price": median_price, "highest_price": highest_price})
            except AttributeError:
                # do nothing
                pass
            print(last_sold+": "+ median_price)

    # Convert data list into pandas DataFrame
    df = pd.DataFrame(data)

    # Convert the "lowest_price", "median_price", and "highest_price" columns to floats
    df[["lowest_price", "median_price", "highest_price"]] = df[["lowest_price", "median_price", "highest_price"]].applymap(lambda x: float(x.lstrip("$")))

    # Convert the date column to datetime format
    df['date'] = pd.to_datetime(df['date'])

    # Group rows by date and calculate the mean of the "lowest_price", "median_price", and "highest_price" columns for each group
    df = df.groupby("date").mean().reset_index().sort_values('date').set_index('date')

    # print(df)

    if plot:
        # sns.boxplot(x=df['lowest_price'])
        sns.displot(df["lowest_price"], bins=10,kde=False)
        plt.title('Before Winsorize')
        plt.show()

    # apply winsorize to each column separately
    for col in df.columns:
        df[col] = winsorize(df[col], limits=(0.01, 0.02))

    if plot:
        # sns.boxplot(x=df['lowest_price'])
        sns.displot(df["lowest_price"], bins=10,kde=False)
        plt.title('After Winsorize')
        plt.show()

    # Fill in missing dates with NaNs and puts previous average:
    df = df.resample('D').mean()

    # replace NaN values with expanding mean
    df = df.fillna(df.expanding().mean())

    if write_csv:
        # convert dataframe to csv file
        df.to_csv('data.csv')

    # print(df)
    return df

if __name__ == "__main__":
    load_prices_discogs("29735", "Parachutes", write_csv=True, plot=True)
