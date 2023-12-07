import os




os.chdir('/nfsscratch/Users/wndrsn')
access_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InduZHJzbjIiLCJleHAiOjE3MDYyMzc4MzAsImlhdCI6MTcwMTA1MzgzMCwiaXNzIjoiRWFydGhkYXRhIExvZ2luIn0.xTFpkR50tS67DBsdP0vXY1Lv-yMPkr244434jJhAhokA1SuaIPnUSep7VWqjvk4e769S0N6dS6k4AhHqmDDIyQWJ9Hid6I1oxF6t-FQiqpbI_8LgSK3CiXdQbkOzoTknT045nE-3lvFizxmYiwILNoCPD71VRAajc6Mxksa39AGeC0phXBOVTWpgzTxTBN8KZfPagDjdkNHOQNzBd2MoS7Lw4UKZ9Q5qUZHMx_hM-oPKpob-ojWnk5mCgW31Jri3KrmaKEUm02ULcrF8015DqHrWtyjJZxRro4PWlF2lmSw5m59P2oBhwwgjW5qdmxNwLfv-ePfop8VDXLzMKlC-wQ'

 


def wipe_directory():    
    command_1 ="rm -rf /nfsscratch/Users/wndrsn/MOD06_L2"
    os.system(command_1)
    print('Removed MOD06_L2 directory')


def main():
    days = ['00' + str(_) for _ in range(1,10)] + ['0' + str(_) for _ in range(10,100)] + [str(_) for _ in range(100,366)]
    wipe_directory()
    #years = ["2018","2019" "2020", "2021", "2022", "2023"]
    years = '2019'
    for day in days: 
        print(f'processing day {day}...')
        url = f"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD06_L2/{years}/{day}/"
        command = f"wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 {url} --header 'Authorization: Bearer {access_token}' -P ."
        os.system(command)
        print(f'Finished processing day {day}')
        


if __name__ == "__main__":
    main()
