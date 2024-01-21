import os

os.chdir('/nfsscratch/Users/wndrsn')
access_token = ''


def wipe_directory():    
    command_1 ="rm -rf /nfsscratch/Users/wndrsn/MOD06_L2"
    os.system(command_1)
    print('Removed MOD06_L2 directory')
    days = ['00' + str(_) for _ in range(1,10)] + ['0' + str(_) for _ in range(10,100)] + [str(_) for _ in range(100,366)]
    for day in days:
        command_2 = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD06_L2/2019/{day}/" --header "Authorization: Bearer {access_token}" -P .'
        os.system(command_2)
