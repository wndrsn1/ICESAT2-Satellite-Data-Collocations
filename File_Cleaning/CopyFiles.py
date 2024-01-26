import os
import glob
import shutil
from datetime import datetime
import pandas as pd
import tqdm

def duplicate_h5_and_hdf_files(source_folder, destination_folder, file_patterns):
    try:
        for file_pattern in tqdm(file_patterns):
            # Find all H5 and HDF files in the source folder based on the pattern
            h5_files = glob.glob(os.path.join(source_folder, f'**/*{file_pattern}'), recursive=True)

            # Duplicate each H5 and HDF file to the destination folder
            for source_file in h5_files:
                try:
                    # Generate the destination file path
                    destination_file = os.path.join(destination_folder, os.path.basename(source_file))

                    # Copy the H5 file
                    shutil.copy(source_file, destination_file)

                    # # Get the current date for logging
                    # current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"File {source_file} duplicated successfully as {destination_file}.")
                except Exception as e:
                    print(f"Error: {e}")

    except Exception as e:
        print(f"Error: {e}")

# Example usage
source_folder_path = "/nfsscratch/Users/wndrsn"
filename = 'Colocation_Files'
destination_folder_path = f"/nfsscratch/Users/wndrsn/{filename}"
if os.path.exists(destination_folder_path) == False:
    os.mkdir(destination_folder_path) 
test = pd.read_csv(os.path.join('/Users/wndrsn',(filename +'.csv')), usecols=['filename_right', 'filename_left'])
Right_files = test['filename_right'].drop_duplicates()
Left_files = test['filename_left'].drop_duplicates()
atl_files = pd.DataFrame()
atl_files = atl_files.append([Left_files,Right_files])

print('Creating Colocation_Files.csv.. ')
atl_files.to_csv(f'/Users/wndrsn/{filename}.csv')
print('Created Colocation_Files.csv!')

file_patterns = atl_files.tolist()

duplicate_h5_and_hdf_files(source_folder_path, destination_folder_path, file_patterns)
