import os
import argparse

parser = argparse.ArgumentParser(description="Set directory to the specified path")
parser.add_argument("path", type=str, help="Path to set the directory to")
parser.add_argument("access_token", type=str, help="Enter your Earth Data access token")
args = parser.parse_args()

path = args.path
os.chdir(path)

access_token = args.access_token

def Download_CALIPSO_data():    
    os.chdir('/nfsscratch/Users/wndrsn/CALIPSOData')
    months = ['0' + str(_) for _ in range(1,10)] + [str(_) for _ in range(10,13)]
    years = ['2019','2020','2021','2022','2023']
    for year in years:
        for month in months:
            command_1 = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://asdc.larc.nasa.gov/data/CALIPSO/LID_L2_333mMLay-Standard-V4-51/{year}/{month}/" --header "Authorization: Bearer {access_token}" -P .'
            os.system(command_1)

def Download_ATL09_data():
    os.chdir('/nfsscratch/Users/wndrsn/ATL09')
    days = ['0' + str(i) for i in range(1,9)] + [ i for i in range(10,32)]
    months = ['0' + str(i) for i in range(1,9)] + [ i for i in range(10,13)]
    years = ['2019','2020','2021','2022','2023']
    for year in years:
        for month in months:
            for day in days:
                command_1 = f'wget -A h5 --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies --no-check-certificate --auth-no-challenge=on -r --reject ".h5*"-np -e robots=off https://n5eil01u.ecs.nsidc.org/ATLAS/ATL09.006/2018.10.13/'
                os.system(command_1)

Download_CALIPSO_data()
Download_ATL09_data()
