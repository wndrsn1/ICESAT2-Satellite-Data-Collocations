import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import os

filenames = ['colocationsListFinal2019','colocationsListFinal2020','colocationsListFinal2021']

# Initialize an empty list to store GeoDataFrames
gdf_list = []

for filename in filenames:
    output_shapefile_left = f'ATL_{filename}.shp'  # Add an underscore for better naming
    test = pd.read_csv(os.path.join('/Users/wndrsn', (filename + '.csv')),
                       usecols=['filename_right', 'filename_left', 'Long0_left', 'Lat0_left','Time0_left'])
    test = test.drop_duplicates(subset=['filename_left'], keep='first', inplace=False, ignore_index=False)
    test = test.drop_duplicates(subset=['filename_right'], keep='first', inplace=False, ignore_index=False)
    
    # Process the left side
    geometry_left = [Point(xy) for xy in zip(test['Long0_left'], test['Lat0_left'])]
    gdf_left2 = gpd.GeoDataFrame(test, geometry=geometry_left, crs='EPSG:4326')

    # Append gdf_left2 to the list
    gdf_list.append(gdf_left2)

# Concatenate all GeoDataFrames in the list
gdf_left = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs='EPSG:4326')

# Save the final GeoDataFrame to a shapefile
gdf_left.to_file(output_shapefile_left, mode='w', driver='ESRI Shapefile')
