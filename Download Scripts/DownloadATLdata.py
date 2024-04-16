import requests
import os
from bs4 import BeautifulSoup
from multiprocessing import Pool

# Function to download a file
def download_file(url, destination_folder):
    filename = url.split("/")[-1]
    destination_path = os.path.join(destination_folder, filename)
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(destination_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"Downloaded {filename} to {destination_folder}")

# Function to get list of file URLs for a given date
def get_file_urls(year, month, day):
    url_base = f"https://n5eil01u.ecs.nsidc.org/ATLAS/ATL02.006/{year}.{month}.{day}/"
    response = requests.get(url_base)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    file_urls = [url_base + link['href'] for link in links if link['href'].endswith('.h5')]
    return file_urls

# Main function to download files using multiprocessing
def download_files_for_date(date):
    year, month, day = date
    file_urls = get_file_urls(year, month, day)
    for url in file_urls:
        download_file(url, destination_folder)

# Destination folder to save downloaded files
destination_folder = "/nfsscratch/Users/wndrsn/atl02"

# Create the destination folder if it doesn't exist
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# List of dates to process
dates = [(year, f"{month:02d}", f"{day:02d}") for year in range(2019, 2021) for month in range(1, 13) for day in range(1, 32)]

# Use multiprocessing to download files concurrently
with Pool(processes=4) as pool:
    pool.map(download_files_for_date, dates)
    
