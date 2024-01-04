import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import os
import glob

os.chdir('/Users/wndrsn/')
path = '/nfsscratch/Users/wndrsn/2019_Shapefiles'
search_text = '2019'
filetype = '.csv'
files = glob.glob(os.path.join(path, '**/*{filetype}'), recursive=True)
files = [file for file in files if search_text in file.lower() and file.endswith(filetype)]
for filepath in tqdm(files):
    # Extract the file name without extension
    file_name = os.path.splitext(os.path.basename(filepath))[0]

    # Output shapefile paths
    output_shapefile_left = f'ATL_{file_name}.shp'  # Add an underscore for better naming
    output_shapefile_right = f'MODIS_{file_name}.shp'  # Add an underscore for better naming

    # Read the entire CSV
    data = pd.read_csv(filepath, usecols=['Long0_right', 'Lat0_right', 'Long0_left', 'Lat0_left'])
    print('CSV file read!')

    # Process the right side
    geometry_right = [Point(xy) for xy in zip(data['Long0_right'], data['Lat0_right'])]
    gdf_right = gpd.GeoDataFrame(data, geometry=geometry_right, crs='EPSG:4326')
    gdf_right.to_file(output_shapefile_right, mode='w', driver='ESRI Shapefile')
    print('Created MODIS shapefile')

    # Process the left side
    geometry_left = [Point(xy) for xy in zip(data['Long0_left'], data['Lat0_left'])]
    gdf_left = gpd.GeoDataFrame(data, geometry=geometry_left, crs='EPSG:4326')
    gdf_left.to_file(output_shapefile_left, mode='w', driver='ESRI Shapefile')

    print(f'Shapefiles saved at: {output_shapefile_left}, {output_shapefile_right}')
    break
