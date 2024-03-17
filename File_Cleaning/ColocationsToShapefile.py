import os
import glob
import pandas as pd
from tqdm import tqdm
from shapely import Point
import geopandas as gpd
import argparse

# Create an argument parser
parser = argparse.ArgumentParser(description="Set directory to the specified path")

# Add an argument for the path
parser.add_argument("path", type=str, help="Path to set the directory to")

# Parse the arguments
args = parser.parse_args()

# Set the directory to the specified path
path = args.path

filetype = '.csv'
# List all files in the directory
all_files = glob.glob(os.path.join(path, f'**/*{filetype}'), recursive=True)

gdf_list = []
output_shapefile = f'Collocations_2019.shp'  
for filename in tqdm(all_files):
    
    test = pd.read_csv(os.path.join(filename,usecols=['Lat0_left','Long0_left']))
    
    # Process the left side
    geometry_left = [Point(xy) for xy in zip(test['Long0_right'], test['Lat0_right'])]
    gdf_left2 = gpd.GeoDataFrame(test, geometry=geometry_left, crs='EPSG:4326')

    # Append gdf_left2 to the list
    gdf_list.append(gdf_left2)
    print(f'Finished processing {filename}!')

# Concatenate all GeoDataFrames in the list
gdf_left = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs='EPSG:4326')
df_left = pd.concat(gdf_list,ignore_index = True)
df_left.to_csv(f'{output_shapefile}.csv')
# Save the final GeoDataFrame to a shapefile
gdf_left.to_file(output_shapefile, mode='w', driver='ESRI Shapefile')
