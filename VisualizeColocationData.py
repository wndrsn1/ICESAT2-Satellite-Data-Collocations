import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import os
 
os.chdir('/Users/wndrsn/')
# File path
filepath = '/Users/wndrsn/colocationsListFinal2019.csv'

# Output shapefile paths
output_shapefile_left = 'ATL_shapefile_2019.shp'
output_shapefile_right = 'MODIS_shapefile_2019.shp'

# Read CSV in chunks
chunk_size = 10000  # Adjust this based on your system's memory
chunks = pd.read_csv(filepath, usecols= ['Long0_right','Lat0_right'])

# Process each chunk and create shapefiles
for chunk in tqdm(chunks, desc='Processing chunks'):
    # Right side
    geometry_right = [Point(xy) for xy in zip(chunk['Long0_right'], chunk['Lat0_right'])]
    gdf_right = gpd.GeoDataFrame(chunk, geometry=geometry_right, crs='EPSG:4326')
    gdf_right.to_file(output_shapefile_right, mode='w', driver='ESRI Shapefile')

del chunks, geometry_right, gdf_right

chunks = pd.read_csv(filepath, usecols= ['Long0_right','Lat0_right'])

# Process each chunk and create shapefiles
for chunk in tqdm(chunks, desc='Processing chunks'):

    # Left side
    geometry_left = [Point(xy) for xy in zip(chunk['Long0_left'], chunk['Lat0_left'])]
    gdf_left = gpd.GeoDataFrame(chunk, geometry=geometry_left, crs='EPSG:4326')
    gdf_left.to_file(output_shapefile_left, mode='w', driver='ESRI Shapefile')

print(f'Shapefiles saved at: {output_shapefile_left}, {output_shapefile_right}')
