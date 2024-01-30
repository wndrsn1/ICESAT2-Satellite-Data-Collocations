import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
import os
years = ['2019','2020','2021']
filenames = [f'colocationsListFinal{year}' for year in years]
def create_shapefile(columnsList,side):
    # Initialize an empty list to store GeoDataFrames
    gdf_list = []

    for filename in filenames:
        output_shapefile_left = f'CollocationsRight.shp'  # Add an underscore for better naming
        test = pd.read_csv(os.path.join('/Users/wndrsn', (filename + '.csv')),
                        usecols=columnsList)
        test = test.drop_duplicates(subset=['filename_left'], keep='first', inplace=False, ignore_index=False)
        test = test.drop_duplicates(subset=['filename_right'], keep='first', inplace=False, ignore_index=False)
        
        # Process the left side
        geometry_left = [Point(xy) for xy in zip(test['Long0_right'], test['Lat0_right'])]
        gdf_left2 = gpd.GeoDataFrame(test, geometry=geometry_left, crs='EPSG:4326')

        # Append gdf_left2 to the list
        gdf_list.append(gdf_left2)
        print(f'Finished processing {filename}!')

    # Concatenate all GeoDataFrames in the list
    gdf_left = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs='EPSG:4326')
    df_left = pd.concat(gdf_list,ignore_index = True)
    df_left.to_csv(f'Colocations_list_{side}.csv')
    # Save the final GeoDataFrame to a shapefile
    gdf_left.to_file(output_shapefile_left, mode='w', driver='ESRI Shapefile')

if __name__ == "__main__":
    create_shapefile(['filename_left','filename_right', 'Long0_right', 'Lat0_right','Time0_right'],'Left')
    create_shapefile(['filename_left','filename_right', 'Long0_left', 'Lat0_left','Time0_left'],'Right')


