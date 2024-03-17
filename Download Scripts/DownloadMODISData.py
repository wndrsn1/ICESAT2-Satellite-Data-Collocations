import os
import argparse

# Create an argument parser
parser = argparse.ArgumentParser(description="Set directory to the specified path")

# Add an argument for the path
parser.add_argument("path", type=str, help="Path to set the directory to")

# Parse the arguments
args = parser.parse_args()

# Set the directory to the specified path
path = args.path
os.chdir(path)

access_token = input('Please enter Earthdata access token')


def Download_data():    
    days = ['00' + str(_) for _ in range(1,10)] + ['0' + str(_) for _ in range(10,100)] + [str(_) for _ in range(100,366)]
    years = ['2019','2020','2021','2022','2023']
    for year in years:
        for day in days:
            command_1 = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD06_L2/{year}/{day}/" --header "Authorization: Bearer {access_token}" -P .'
            command_2 = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MYD06_L2/{year}/{day}/" --header "Authorization: Bearer {access_token}" -P .'
            os.system(command_1)
            os.system(command_2)
