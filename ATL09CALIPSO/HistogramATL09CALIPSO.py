import glob
import os
import pandas as pd
import numpy as np
import h5py
import xarray as xr
from datetime import datetime, timedelta
import dask.array as da
import matplotlib.pyplot as plt
import argparse

# Create an argument parser
parser = argparse.ArgumentParser(description="Set directory to the specified path")

# Add an argument for the path
parser.add_argument("path", type=str, help="Path to set the directory to")

# Parse the arguments
args = parser.parse_args()

# Set the directory to the specified path
os.chdir(args.path)

path = args.path

def XRenderMODIS(filename):
        dataset = xr.open_dataset(filename, engine="netcdf4", drop_variables = full_list)
        lat = np.array(dataset['Latitude']).flatten()
        long = np.array(dataset['Longitude']).flatten()
        time = np.array(dataset['Profile_Time']).flatten()
        ProfileID = np.array(dataset['Profile_ID']).flatten()
        lat = pd.DataFrame(lat, columns=['Lat0'])
        long = pd.DataFrame(long, columns=['Long0'])
        time = pd.DataFrame(time, columns=['Time0'])
        ProfileID = pd.DataFrame(ProfileID, columns=['Profile_ID'])
        data = pd.concat((lat, long, time,ProfileID), axis=1)
        data['Time0'] = data['Time0'].apply(days_to_date)
        filename_df = pd.DataFrame([os.path.basename(filename)] * len(data), columns=['filename'])
        data = pd.concat([data, filename_df], axis=1)
        print(f'{filename} successfully processed!')
        dataset.close()
        return data

def HDFtoDF(filename, chunk_size=408):
    try:
        # Open the HDF file using h5py
        with h5py.File(filename, 'r') as file:
            # Access latitude, longitude, and time data using Dask
            latitude_data = da.from_array(file['profile_1']['high_rate']['latitude'],chunks = chunk_size)
            longitude_data = da.from_array(file['profile_1']['high_rate']['longitude'], chunks=chunk_size)
            delta_time_data = da.from_array(file['profile_1']['high_rate']['delta_time'], chunks=chunk_size)

            # Convert Dask arrays to Pandas DataFrame
            df = pd.DataFrame({
                'Lat0': latitude_data.compute(),
                'Long0': longitude_data.compute(),
                'Time0': delta_time_data.compute()
            })
            df['Time0'] = df['Time0'].apply(gps_to_datetime)
            filename_df = pd.DataFrame([os.path.basename(filename)] * len(df), columns=['filename'])
            df = pd.concat([df, filename_df], axis=1)
            print(f"{filename} successfully processed!")
        return df

    except Exception as e:
        print(f'Error reading {filename} due to {e}')




def gps_to_datetime(gps_seconds):
    gps_epoch = datetime(2018, 1, 1)
    gps_time = gps_epoch + timedelta(seconds=gps_seconds)
    return gps_time

def CALtoDayYear(yyyymmdd):
    yyyymmdd = yyyymmdd[72:76] + yyyymmdd[77:79] + yyyymmdd[80:82]
    date_object = datetime.strptime(yyyymmdd, '%Y%m%d')
    # Get the day of the year
    day_of_year = date_object.timetuple().tm_yday
    return day_of_year


def day_of_year(yyyymmdd):
    yyyymmdd = (yyyymmdd[42:50])
    # Convert the input string to a datetime object
    date_object = datetime.strptime(yyyymmdd, '%Y%m%d')

    # Get the day of the year
    day_of_year = date_object.timetuple().tm_yday

    return day_of_year

def days_to_date(input_str):
    # Define the reference date (1993, 1, 1)
    gps_epoch = datetime(1993, 1, 1)
    gps_time = gps_epoch + timedelta(seconds=input_str)
    return gps_time


def getFileTime():
    dateResultDF = pd.DataFrame([])
    CALIPSOFiles = glob.glob(os.path.join(path, '**/*.hdf'), recursive=True)
    CALIPSO = pd.DataFrame({'filename':CALIPSOFiles})
    CALIPSO['day'] = CALIPSO['filename'].apply(CALtoDayYear)
    CALIPSO['year'] = CALIPSO['filename'].str.slice(72,76)
    print('MODIS Data found!')


    # Concatenate results to dateResultDF
    dateResultDF = pd.concat([dateResultDF, CALIPSO[['year', 'day','filename']]], ignore_index=True)
    atlfiles = glob.glob(os.path.join(path, '*.h5'))
    
    atl_pd = pd.DataFrame({'filename':atlfiles})
    atl_pd['day'] = atl_pd['filename'].apply(day_of_year)
    atl_pd['year'] = atl_pd['filename'].str.slice(42,46)
    dateResultDF = pd.concat((dateResultDF,atl_pd))
    print('ATL data found!')
    return dateResultDF


def process_files_for_month(file):
    if file.endswith('.h5'): return HDFtoDF(file)
    if file.endswith('.hdf'): return XRenderMODIS(file)


def main():
    years = ["2019", "2020"]
    days = ['00' + str(_) for _ in range(1,10)] + ['0' + str(_) for _ in range(10,100)] + [str(_) for _ in range(100,366)]
    print('Getting files...')
    filetimes = getFileTime()
    print('Files found!')
    filetimes['year'] = filetimes['year'].astype(str)
    filetimes['day'] = filetimes['day'].astype(str)
    for year in years:
        print(f'Processing year: {year}')
        i = 1
        for day in days:
            files = filetimes[(filetimes['year'] == str(year)) & ((filetimes['day'] == str(day)) | (filetimes['day'] == str(i)))]
            files = files['filename']
            CALIPSO_data = pd.concat([XRenderMODIS(file) for file in files if file.endswith('.hdf')], ignore_index=True)
            ATL_data = pd.concat([HDFtoDF(file) for file in files if file.enswith('.h5')], ignore_index=True)
    all_latitudes = np.array(pd.concat([CALIPSO_data['Lat0'],ATL_data['Lat0']]))
    all_longitudes = np.array(pd.concat([CALIPSO_data['Long0'],ATL_data['Long0']]))

    # Plot histograms
    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    plt.hist(all_latitudes, bins=50, color='blue', alpha=0.7)
    plt.xlabel('Latitude')
    plt.ylabel('Frequency')
    plt.title('Histogram of Latitude over Time')

    plt.subplot(1, 2, 2)
    plt.hist(all_longitudes, bins=50, color='green', alpha=0.7)
    plt.xlabel('Longitude')
    plt.ylabel('Frequency')
    plt.title('Histogram of Longitude over Time')

    plt.tight_layout()
    plt.show()



full_list = ['Profile_UTC_Time', 'Day_Night_Flag', 'Minimum_Laser_Energy_532', 'Column_Optical_Depth_Cloud_532', 'Column_Optical_Depth_Cloud_Uncertainty_532', 'Column_Optical_Depth_Tropospheric_Aerosols_532', 'Column_Optical_Depth_Tropospheric_Aerosols_Uncertainty_532', 'Column_Optical_Depth_Stratospheric_Aerosols_532', 'Column_Optical_Depth_Stratospheric_Aerosols_Uncertainty_532', 'Column_Optical_Depth_Tropospheric_Aerosols_1064', 'Column_Optical_Depth_Tropospheric_Aerosols_Uncertainty_1064', 'Column_Optical_Depth_Stratospheric_Aerosols_1064', 'Column_Optical_Depth_Stratospheric_Aerosols_Uncertainty_1064', 'Column_Feature_Fraction', 'Column_Integrated_Attenuated_Backscatter_532', 'Column_IAB_Cumulative_Probability', 'Tropopause_Height', 'Tropopause_Temperature', 'Temperature', 'Pressure', 'Molecular_Number_Density', 'Ozone_Number_Density', 'Relative_Humidity', 'IGBP_Surface_Type', 'Surface_Elevation_Statistics', 'Surface_Winds', 'Samples_Averaged', 'Aerosol_Layer_Fraction', 'Cloud_Layer_Fraction', 'Atmospheric_Volume_Description', 'Extinction_QC_Flag_532', 'Extinction_QC_Flag_1064', 'CAD_Score', 'Total_Backscatter_Coefficient_532', 'Total_Backscatter_Coefficient_Uncertainty_532', 'Perpendicular_Backscatter_Coefficient_532', 'Perpendicular_Backscatter_Coefficient_Uncertainty_532', 'Particulate_Depolarization_Ratio_Profile_532', 'Particulate_Depolarization_Ratio_Uncertainty_532', 'Extinction_Coefficient_532', 'Extinction_Coefficient_Uncertainty_532', 'Aerosol_Multiple_Scattering_Profile_532', 'Backscatter_Coefficient_1064', 'Backscatter_Coefficient_Uncertainty_1064', 'Extinction_Coefficient_1064', 'Extinction_Coefficient_Uncertainty_1064', 'Aerosol_Multiple_Scattering_Profile_1064', 'Surface_Top_Altitude_532', 'Surface_Base_Altitude_532', 'Surface_Integrated_Attenuated_Backscatter_532', 'Surface_532_Integrated_Depolarization_Ratio', 'Surface_532_Integrated_Attenuated_Color_Ratio', 'Surface_Detection_Flags_532', 'Surface_Detection_Confidence_532', 'Surface_Overlying_Integrated_Attenuated_Backscatter_532', 'Surface_Scaled_RMS_Background_532', 'Surface_Peak_Signal_532', 'Surface_Detections_333m_532', 'Surface_Detections_1km_532', 'Surface_Top_Altitude_1064', 'Surface_Base_Altitude_1064', 'Surface_Integrated_Attenuated_Backscatter_1064', 'Surface_1064_Integrated_Depolarization_Ratio', 'Surface_1064_Integrated_Attenuated_Color_Ratio', 'Surface_Detection_Flags_1064', 'Surface_Detection_Confidence_1064', 'Surface_Overlying_Integrated_Attenuated_Backscatter_1064', 'Surface_Scaled_RMS_Background_1064', 'Surface_Peak_Signal_1064', 'Surface_Detections_333m_1064', 'Surface_Detections_1km_1064']


main()
