import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from tqdm import tqdm
from retrying import retry



os.chdir('/nfsscratch/Users/wndrsn/MODL602')
access_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InduZHJzbjIiLCJleHAiOjE3MDYyMzc4MzAsImlhdCI6MTcwMTA1MzgzMCwiaXNzIjoiRWFydGhkYXRhIExvZ2luIn0.xTFpkR50tS67DBsdP0vXY1Lv-yMPkr244434jJhAhokA1SuaIPnUSep7VWqjvk4e769S0N6dS6k4AhHqmDDIyQWJ9Hid6I1oxF6t-FQiqpbI_8LgSK3CiXdQbkOzoTknT045nE-3lvFizxmYiwILNoCPD71VRAajc6Mxksa39AGeC0phXBOVTWpgzTxTBN8KZfPagDjdkNHOQNzBd2MoS7Lw4UKZ9Q5qUZHMx_hM-oPKpob-ojWnk5mCgW31Jri3KrmaKEUm02ULcrF8015DqHrWtyjJZxRro4PWlF2lmSw5m59P2oBhwwgjW5qdmxNwLfv-ePfop8VDXLzMKlC-wQ'


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def download_data(daysandyears):
    year, day = daysandyears
    url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD06_L2/{year}/{day}/"
    command = f"wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 {url} --header 'Authorization: Bearer {access_token}' -P ."
    os.system(command)


def main():
    days = ['00' + str(_) for _ in range(1,10)] + ['0' + str(_) for _ in range(10,100)] + [str(_) for _ in range(100,366)]

    years = ["2018","2019" "2020", "2021", "2022", "2023"]
    with ProcessPoolExecutor(max_workers=6) as executor:
        # Use a list to store the futures
        futures = [executor.submit(download_data, (year, day)) for year in years for day in days]

    # Wait for all futures to complete
    concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()


#wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD06_L2/2000/055/MOD06_L2.A2000055.0000.061.2017272125816.hdf/" --header "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InduZHJzbjIiLCJleHAiOjE3MDYyMzc4MzAsImlhdCI6MTcwMTA1MzgzMCwiaXNzIjoiRWFydGhkYXRhIExvZ2luIn0.xTFpkR50tS67DBsdP0vXY1Lv-yMPkr244434jJhAhokA1SuaIPnUSep7VWqjvk4e769S0N6dS6k4AhHqmDDIyQWJ9Hid6I1oxF6t-FQiqpbI_8LgSK3CiXdQbkOzoTknT045nE-3lvFizxmYiwILNoCPD71VRAajc6Mxksa39AGeC0phXBOVTWpgzTxTBN8KZfPagDjdkNHOQNzBd2MoS7Lw4UKZ9Q5qUZHMx_hM-oPKpob-ojWnk5mCgW31Jri3KrmaKEUm02ULcrF8015DqHrWtyjJZxRro4PWlF2lmSw5m59P2oBhwwgjW5qdmxNwLfv-ePfop8VDXLzMKlC-wQ" -P .
