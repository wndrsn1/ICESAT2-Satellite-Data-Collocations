import glob
import os
import pandas as pd
import numpy as np
import h5py
import xarray as xr
from datetime import datetime, timedelta
from shapely.geometry import Point
import geopandas as gpd
import concurrent.futures
import dask.array as da


path = r"/nfsscratch/Users/wndrsn"
os.chdir(path)

def colocations_main(atl_data, modis_data, time_threshold_hours=48):

    threshold_distance = 0.2

    # Create GeoDataFrame for ATL data
    geometry_atl = [Point(xy) for xy in zip(atl_data['Long0'], atl_data['Lat0'])]
    gdf_atl = gpd.GeoDataFrame(atl_data, geometry=geometry_atl)

    # Create GeoDataFrame for MODIS data
    geometry_modis = [Point(xy) for xy in zip(modis_data['Long0'], modis_data['Lat0'])]
    gdf_modis = gpd.GeoDataFrame(modis_data, geometry=geometry_modis)

    # Perform a spatial join based on proximity with a specified threshold distance
    result = gdf_atl.sjoin_nearest(gdf_modis, how='inner',max_distance = threshold_distance)


    # Print or log the column names in the resulting GeoDataFrame
    print(result.columns)
    result['Time0_left'] = pd.to_datetime(result['Time0_left'])
    result['Time0_right'] = pd.to_datetime(result['Time0_right'])

    # Adjust the filtering based on the actual column names in the result
    if 'Time0_left' in result.columns and 'Time0_right' in result.columns:
        # Filter based on time values if time_threshold_hours is provided
        if time_threshold_hours is not None:
            result = result[
                (result['Time0_left'] == result['Time0_right']) &
                (result.geometry.distance(result.geometry) <= threshold_distance) &
                (abs((result['Time0_left'] - result['Time0_right']).dt.total_seconds()) / 3600 <= time_threshold_hours)
            ]
        else:
            # Filter only based on spatial threshold if time_threshold_hours is None
            result = result[result.geometry.distance(result.geometry) <= threshold_distance]
    else:
        print("Time columns not found in the result.")

    return result

    
def XRenderMODIS(filename):
    try:
        dataset = xr.open_dataset(filename, engine="netcdf4", drop_variables = full_list)
        lat = np.array(dataset['Latitude'])
        long = np.array(dataset['Longitude'])
        time = np.array(dataset['Scan_Start_Time'])

        lat = pd.DataFrame(name(lat, 'Lat'))['Lat0']
        long = pd.DataFrame(name(long, 'Long'))['Long0']
        time = pd.DataFrame(name(time, 'Time'))['Time0']

        data = pd.DataFrame([])
        data = pd.concat((lat, long, time), axis=1)
        filename_df = pd.DataFrame([os.path.basename(filename)] * len(data), columns=['filename'])
        data = pd.concat([data, filename_df], axis=1)
        print(f'{filename} successfully processed!')
        dataset.close()
        return data
    except Exception as e:
        print(f'error reading {filename} due to {e}')
        
def HDFtoDF(filename, chunk_size=408):
    try:
        # Open the HDF file using h5py
        with h5py.File(filename, 'r') as file:
            # Access latitude, longitude, and time data using Dask
            latitude_data = da.from_array(file['profile_1']['latitude'], chunks=chunk_size)
            longitude_data = da.from_array(file['profile_1']['longitude'], chunks=chunk_size)
            delta_time_data = da.from_array(file['profile_1']['delta_time'], chunks=chunk_size)

            # Convert Dask arrays to Pandas DataFrame
            df = pd.DataFrame({
                'Lat0': latitude_data.compute(),
                'Long0': longitude_data.compute(),
                'Time0': delta_time_data.compute()
            })
            df['Time0'] = df['Time0'].apply(gps_to_datetime)
            print(f"{filename} successfully processed!")
        return df

    except Exception as e:
        print(f'Error reading {filename} due to {e}')

def name(file, label):
    file = pd.DataFrame(file)
    data = pd.DataFrame()
    rows, columns = file.shape
    for column in range(columns):
        nameddata = pd.DataFrame()
        nameddata[f'{label}{column}'] = file[column]
        data = pd.concat((data, nameddata), axis=1)
    return data


def gps_to_datetime(gps_seconds):
    gps_epoch = datetime(2018, 1, 1)
    gps_time = gps_epoch + timedelta(seconds=gps_seconds)
    return gps_time



def gregorian_day_to_month(year, day_of_year):
    # Create a datetime object for the given year and day_of_year
    date_object = datetime.strptime(f'{year}-{day_of_year}', '%Y-%j')

    # Extract the month from the datetime object
    month = date_object.month
    return month


def getFileTime():
    dateResultDF = pd.DataFrame([])
    modfiles = glob.glob(os.path.join('/nfsscratch/Users/wndrsn/MODL602/MOD06_L2', '**/*.hdf'), recursive=True)
    mod_pd = pd.DataFrame({'filename':modfiles})
    mod_pd['day'] = mod_pd['filename'].str.slice(47,50)
    mod_pd['year'] = mod_pd['filename'].str.slice(42,46)

    # Apply gregorian_day_to_month to each row
    mod_pd['month'] = mod_pd.apply(lambda row: gregorian_day_to_month(row['year'], row['day']), axis=1)

    # Concatenate results to dateResultDF
    dateResultDF = pd.concat([dateResultDF, mod_pd[['year', 'month','filename']]], ignore_index=True)
    atlfiles = glob.glob(os.path.join(path, '**/*.h5'), recursive=True)
    
    atl_pd= pd.DataFrame({'filename':atlfiles})
    atl_pd['year'] = atl_pd['filename'].str.slice(31,35)
    atl_pd['month'] = atl_pd['filename'].str.slice(35,37)
    dateResultDF = pd.concat((dateResultDF,atl_pd))
    return dateResultDF


def process_files_for_month(file):
    if file.endswith('.h5'): return HDFtoDF(file)
    if file.endswith('.hdf'): return XRenderMODIS(file)


def main():
    years = ['2019']
    months = ['1','2','3']
    print('Getting files...')
    filetimes = getFileTime()
    print('Files found!')
    filetimes['year'] = filetimes['year'].astype(str)
    filetimes['month'] = filetimes['month'].astype(str)

    # Process files for each month using ProcessPoolExecutor
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Use a list to store the futures
        modis_results = []  # Initialize an empty list before the loop
        atl_results = []
        for year in years:
            for month in months:
                files = filetimes[(filetimes['year'] == str(year)) & ((filetimes['month'] == str(month)) | (filetimes['month'] == str(0)+str(month)))]

                files = files['filename']

                modis_futures = [executor.submit(process_files_for_month, file) for file in files if file.endswith('.hdf')]    
                modis_results = [future.result() for future in concurrent.futures.as_completed(modis_futures)]   

                atl_futures = [executor.submit(process_files_for_month, file) for file in files if file.endswith('.h5')]
                atl_results = [future.result() for future in concurrent.futures.as_completed(atl_futures)]

                # Check if each element in atl_results is a DataFrame before concatenating
                atl_results = [result for result in atl_results if isinstance(result, pd.DataFrame)]
                print(f'atl_results length = {len(atl_results)}')

                # Concatenate the valid DataFrames
                modis_results = pd.concat(modis_results, ignore_index=True)
                atl_results = pd.concat(atl_results, ignore_index=True)
                modis_results = pd.concat((modis_results, DummyTest()), ignore_index=True)
                atl_results = pd.concat((atl_results, DummyTest()), ignore_index=True)
                colocation = pd.DataFrame(colocations_main(atl_results, modis_results))
                if len(colocation) < 4:
                    print('Colocation Failure')

                colocation.to_csv(f'colocations{month}{year}.csv')
                print(f'{month} successfully moved to CSV!')


                
def DummyTest():

    # Create a list of filenames
    filenames = ['filename1.txt', 'filename2.txt', 'filename3.txt', 'filename4.txt', 'filename5.txt']

    # Create a list of latitude values
    lats = [37.783333, 37.338208, 37.774929, 37.529500, 37.421995]

    # Create a list of longitude values
    longs = [-122.416667, -121.886329, -122.419415, -122.271167, -122.083333]

    # Create a list of time values
    times = ['2023-11-28T12:00:00', '2023-11-28T12:10:00', '2023-11-28T12:20:00', '2023-11-28T12:30:00', '2023-11-28T12:40:00']

    # Create a DataFrame
    df = pd.DataFrame({'filename': filenames, 'Lat0': lats, 'Long0': longs, 'Time0': times})

    # Print the DataFrame
    return df

full_list = ['Solar_Zenith', 'Solar_Zenith_Day', 'Solar_Zenith_Night', 'Solar_Azimuth', 
'Solar_Azimuth_Day', 'Solar_Azimuth_Night', 'Sensor_Zenith', 'Sensor_Zenith_Day', 'Sensor_Zenith_Night', 'Sensor_Azimuth', 
'Sensor_Azimuth_Day', 'Sensor_Azimuth_Night', 'Brightness_Temperature', 'Surface_Temperature', 'Surface_Pressure', 'Cloud_Height_Method', 'Cloud_Top_Height', 
'Cloud_Top_Height_Nadir', 'Cloud_Top_Height_Nadir_Day', 'Cloud_Top_Height_Nadir_Night', 'Cloud_Top_Pressure', 'Cloud_Top_Pressure_Nadir', 
'Cloud_Top_Pressure_Night', 'Cloud_Top_Pressure_Nadir_Night', 'Cloud_Top_Pressure_Day', 'Cloud_Top_Pressure_Nadir_Day', 
'Cloud_Top_Temperature', 'Cloud_Top_Temperature_Nadir', 'Cloud_Top_Temperature_Night', 'Cloud_Top_Temperature_Nadir_Night', 
'Cloud_Top_Temperature_Day', 'Cloud_Top_Temperature_Nadir_Day', 'Tropopause_Height', 'Cloud_Fraction', 'Cloud_Fraction_Nadir', 
'Cloud_Fraction_Night', 'Cloud_Fraction_Nadir_Night', 'Cloud_Fraction_Day', 'Cloud_Fraction_Nadir_Day', 'Cloud_Effective_Emissivity', 'Cloud_Effective_Emissivity_Nadir', 
'Cloud_Effective_Emissivity_Night', 'Cloud_Effective_Emissivity_Nadir_Night', 'Cloud_Effective_Emissivity_Day', 'Cloud_Effective_Emissivity_Nadir_Day', 
'Cloud_Top_Pressure_Infrared', 'Spectral_Cloud_Forcing', 'Cloud_Top_Pressure_From_Ratios', 'Radiance_Variance', 'Cloud_Phase_Infrared', 'Cloud_Phase_Infrared_Night', 
'Cloud_Phase_Infrared_Day', 'Cloud_Phase_Infrared_1km', 'IRP_CTH_Consistency_Flag_1km', 'os_top_flag_1km', 'cloud_top_pressure_1km', 'cloud_top_height_1km', 
'cloud_top_temperature_1km', 'cloud_emissivity_1km', 'cloud_top_method_1km', 'surface_temperature_1km', 'cloud_emiss11_1km', 'cloud_emiss12_1km', 'cloud_emiss13_1km', 
'cloud_emiss85_1km', 'Cloud_Effective_Radius', 'Cloud_Effective_Radius_PCL', 'Cloud_Effective_Radius_16', 'Cloud_Effective_Radius_16_PCL', 'Cloud_Effective_Radius_37', 
'Cloud_Effective_Radius_37_PCL', 'Cloud_Optical_Thickness', 'Cloud_Optical_Thickness_PCL', 'Cloud_Optical_Thickness_16', 'Cloud_Optical_Thickness_16_PCL', 'Cloud_Optical_Thickness_37', 
'Cloud_Optical_Thickness_37_PCL', 'Cloud_Effective_Radius_1621', 'Cloud_Effective_Radius_1621_PCL', 'Cloud_Optical_Thickness_1621', 'Cloud_Optical_Thickness_1621_PCL', 'Cloud_Water_Path', 
'Cloud_Water_Path_PCL', 'Cloud_Water_Path_1621', 'Cloud_Water_Path_1621_PCL', 'Cloud_Water_Path_16', 'Cloud_Water_Path_16_PCL', 'Cloud_Water_Path_37', 'Cloud_Water_Path_37_PCL', 'Cloud_Effective_Radius_Uncertainty',
 'Cloud_Effective_Radius_Uncertainty_16', 'Cloud_Effective_Radius_Uncertainty_37', 'Cloud_Optical_Thickness_Uncertainty', 'Cloud_Optical_Thickness_Uncertainty_16', 'Cloud_Optical_Thickness_Uncertainty_37', 
 'Cloud_Water_Path_Uncertainty', 'Cloud_Effective_Radius_Uncertainty_1621', 'Cloud_Optical_Thickness_Uncertainty_1621', 'Cloud_Water_Path_Uncertainty_1621', 'Cloud_Water_Path_Uncertainty_16', 'Cloud_Water_Path_Uncertainty_37', 
 'Above_Cloud_Water_Vapor_094', 'IRW_Low_Cloud_Temperature_From_COP', 'Cloud_Phase_Optical_Properties', 'Cloud_Multi_Layer_Flag', 'Cirrus_Reflectance', 'Cirrus_Reflectance_Flag', 'Cloud_Mask_5km', 'Quality_Assurance_5km', 
 'Cloud_Mask_1km', 'Extinction_Efficiency_Ice', 'Asymmetry_Parameter_Ice', 'Single_Scatter_Albedo_Ice', 'Extinction_Efficiency_Liq', 'Asymmetry_Parameter_Liq', 'Single_Scatter_Albedo_Liq', 'Cloud_Mask_SPI', 'Retrieval_Failure_Metric', 
 'Retrieval_Failure_Metric_16', 'Retrieval_Failure_Metric_37', 'Retrieval_Failure_Metric_1621', 'Atm_Corr_Refl', 'Quality_Assurance_1km', 'Statistics_1km_sds']

main()


# modis_files = glob.glob(os.path.join(path, '**/*.hdf'), recursive=True)
# atl_files = glob.glob(os.path.join(path, '**/*.h5'), recursive=True)

# print(modis_files[0])
# print(atl_files[100])
