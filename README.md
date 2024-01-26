# MODIS/ICESAT2 Satellite Data Project 

![alt text](https://github.com/wndrsn1/MODIS-ICESAT2-Satellite-Data/blob/main/Collocation_Data/Heat%20Density%20Map.png)

This directory downloads and processes MODIS, CALIPSO, and ICESAT2 data to obtain collocations for the years 2019-2023.

In order to use access satellite data, first create an account here: https://www.earthdata.nasa.gov/
  
  Generate a user token and save it for later on
  
With miniconda installed, config.py can be used to set up the virtual environment and libraries needed to run the repository.

The ICESAT2 data download program can be found here https://nsidc.org/data/data-access-tool/ATL04/versions/6
  
  Enter the preferred coordinate and time window and click download. Open the program and paste the user token into the program.

MODIS data can be downloaded using the DownloadMODISData.py program.

CALIPSO data can be downloaded using the DownloadCALIPSO.py program.

Collocations data can be found by using CollocationsByWeek.py, simply enter the relevant file storage and configure collocation and time tolerances. 

  To save memory, the data is analyzed and converted to a CSV file on a week by week basis.

To consolidate the data into a more useful year by year package, use Merge_Collocation_Data.py and enter the location of the previous collocation files. 

It is useful to be able to visualize the data in programs such as ArcGIS pro and QGIS - shapefiles of the data can be created by using CollocationsToShapefile.py 

