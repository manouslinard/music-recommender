from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
import pandas as pd


artist_name = "Queen"
disc_name = "Jazz"

# create Chrome options (no tab):
options = Options()
options.add_argument('--headless')  # remove this if you want the browser to appear.
# create Chrome service - installs chrome driver:
service = Service(ChromeDriverManager().install())
# initialise driver:
driver = webdriver.Chrome(service=service, options=options)
# go to url:
discogs_url = "https://www.discogs.com"

page_url = discogs_url+"/artist/"+artist_name
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

a_tags = soup.find_all('a', href=lambda href: href and '/master/' in href)
for a_tag in a_tags:
    albums[a_tag.text] = discogs_url+a_tag['href']

# print(albums[disc_name])
# driver.get(albums[disc_name])

driver.get(albums[disc_name])
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

buy_button = soup.find('a', string='Buy a copy')
if buy_button:
    buy_link = buy_button.get('href')
    albums[disc_name] = discogs_url+buy_link+"&limit=250"
else:
    print("Disc not found.")
    albums.pop(disc_name)

visited = []
data = []

for n in range(1, 3):
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
        print("Disc not found.")

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
        print(last_sold+": "+ highest_price)

# sort the data by date
data.sort(key=lambda x: x["date"])

# Convert data list into pandas DataFrame
df = pd.DataFrame(data)

# Convert the "lowest_price", "median_price", and "highest_price" columns to floats
df[["lowest_price", "median_price", "highest_price"]] = df[["lowest_price", "median_price", "highest_price"]].applymap(lambda x: float(x.lstrip("$")))

# Group rows by date and calculate the mean of the "lowest_price", "median_price", and "highest_price" columns for each group
grouped = df.groupby("date").mean().reset_index()

# Convert the "lowest_price", "median_price", and "highest_price" columns back to strings
grouped[["lowest_price", "median_price", "highest_price"]] = grouped[["lowest_price", "median_price", "highest_price"]].applymap(lambda x: "${:.2f}".format(x))

# Convert the DataFrame back to a list of dictionaries
data = grouped.to_dict("records")


# write the data to a CSV file
with open('data.csv', 'w', newline='') as csvfile:
    fieldnames = ['date', 'lowest_price', 'median_price', 'highest_price']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for d in data:
        writer.writerow(d)
