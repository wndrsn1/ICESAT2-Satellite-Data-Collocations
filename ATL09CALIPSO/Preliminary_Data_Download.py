import os
import argparse

# Create an argument parser
parser = argparse.ArgumentParser(description="Set directory to the specified path")

# Add an argument for the path
parser.add_argument("path", type=str, help="Path to set the directory to")

# Parse the arguments
args = parser.parse_args()

# Set the directory to the specified path
os.chdir(args.path)

CALIPSO_files = ['CAL_LID_L1-Standard-V4-10.2020-04-02T04-02-09ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-05T04-17-39ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-06T13-54-33ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-06T15-33-09ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-06T17-11-39ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-09T17-27-13ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-11T03-56-28ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-12T16-56-33ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-14T05-04-18ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-15T17-58-18ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-17T06-06-02ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-18T19-06-07ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-04-20T07-13-52ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2020-05-05T13-26-40ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-01-04T17-52-56ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-01-09T07-08-41ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-01-19T23-20-35ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-02-01T02-48-18ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-02-05T15-11-53ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-02-11T17-22-02ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-02-16T05-45-42ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-05-27T14-17-55ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-20T21-20-17ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-20T22-12-27ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-22T11-06-21ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-23T20-49-06ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-25T06-26-00ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-25T10-35-16ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-26T21-04-20ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-28T09-12-00ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-07-29T14-45-29ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-08-01T15-00-39ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-09-15T18-47-27ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-09-18T19-02-31ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-09-29T12-50-41ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-10-05T14-59-15ZN.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-10-20T18-38-44ZD.hdf',
                'CAL_LID_L1-Standard-V4-10.2019-12-16T01-18-44ZN.hdf']

ATL_files = ['ATL09_20200402032743_01010701_006_03.h5',
            'ATL09_20200405034504_01470701_006_03.h5',
            'ATL09_20200406141927_01690701_006_02.h5',
            'ATL09_20200406155344_01700701_006_02.h5',
            'ATL09_20200406172801_01710701_006_02.h5',
            'ATL09_20200409174521_02170701_006_02.h5',
            'ATL09_20200411041944_02390701_006_02.h5',
            'ATL09_20200412162824_02620701_006_02.h5',
            'ATL09_20200414043704_02850701_006_02.h5',
            'ATL09_20200415182001_03090701_006_02.h5',
            'ATL09_20200417062841_03320701_006_02.h5',
            'ATL09_20200418183721_03550701_006_02.h5',
            'ATL09_20200420064601_03780701_006_02.h5',
            'ATL09_20200505125531_06110701_006_02.h5',
            'ATL09_20190104182603_01120201_006_02.h5',
            'ATL09_20190109065200_01810201_006_02.h5',
            'ATL09_20190119230109_03440201_006_02.h5',
            'ATL09_20190201031917_05300201_006_02.h5',
            'ATL09_20190205154517_05990201_006_02.h5',
            'ATL09_20190211175409_06920201_006_02.h5',
            'ATL09_20190216062009_07610201_006_02.h5',
            'ATL09_20190527135146_09060301_006_02.h5',
            'ATL09_20190720203745_03480401_006_02.h5',
            'ATL09_20190720221203_03490401_006_02.h5',
            'ATL09_20190722102043_03720401_006_02.h5',
            'ATL09_20190723205507_03940401_006_02.h5',
            'ATL09_20190725055513_04150401_006_02.h5',
            'ATL09_20190725103805_04180401_006_02.h5',
            'ATL09_20190726211228_04400401_006_02.h5',
            'ATL09_20190728092109_04630401_006_02.h5',
            'ATL09_20190729151240_04820401_006_02.h5',
            'ATL09_20190801135543_05270401_006_02.h5',
            'ATL09_20190915181541_12170401_006_02.h5',
            'ATL09_20190918183301_12630401_006_02.h5',
            'ATL09_20190929121633_00400501_006_02.h5',
            'ATL09_20191005142530_01330501_006_02.h5',
            'ATL09_20191020172626_03640501_006_02.h5',
            'ATL09_20191216005530_12240501_006_01.h5']

access_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InduZHJzbiIsImV4cCI6MTcxNDI0OTI0NCwiaWF0IjoxNzA5MDY1MjQ0LCJpc3MiOiJFYXJ0aGRhdGEgTG9naW4ifQ.0bIIp8ZwuXZThckgonWi1nuQNIT70dmD07vzQFuMhj06bqCdd9zXF-_9m_1zMz2Xgm1d4ZKWHLsSV4-TvHE1dNlbSnv_fH0rJKx_ZByJfw8I9JjJAOzkbnAh6G_qlj88UiR7CDXnGfceJR1H4__HEMkzIhKRs_4a9ynXNvlIyOeb7gIuZPT0tyFhWjTkrsHkr4tBaYhmetXq_UvJZJGhkdy2nLKVr-b9_Upk3HALre6G_S9FPAy5uGuTvVxj4KpHEJARdVDgbUNIAoiEXiNpOLN2AKK9Qa1KJUOrw8JISCUDnJMD58vqQdNALCbsUEd4DX5GKBBc7_VstiZgAp4ZDw'

def Download_data():
    for file in ATL_files:
        year = file[6:10]
        month = file[10:12]
        day = file[12:14]
        command_1 = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://n5eil01u.ecs.nsidc.org/ATLAS/ATL09.006/{year}.{month}.{day}/{file}" --header "Authorization: Bearer {access_token}" -P .'
        os.system(command_1)

    for file in CALIPSO_files:
        year = file[26:30]
        month = file[31:33]
        # day = file[34:36] 
        command_2 = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=3 "https://asdc.larc.nasa.gov/data/CALIPSO/LID_L1-Standard-V4-10/{year}/{month}/{file}" --header "Authorization: Bearer {access_token}" -P .'
        os.system(command_2) 


Download_data()
