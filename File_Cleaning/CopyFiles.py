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

                    # Get the current date for logging
                    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"{current_date}: File '{source_file}' duplicated successfully as '{destination_file}'.")

                except Exception as e:
                    print(f"Error: {e}")

    except Exception as e:
        print(f"Error: {e}")

# Example usage
source_folder_path = "/nfsscratch/Users/wndrsn"
destination_folder_path = "/nfsscratch/Users/wndrsn/ColocationFiles"
test = pd.read_csv('/Users/wndrsn/colocationsListFinal.csv', usecols=['filename_right', 'filename_left'])
test = test.drop_duplicates(subset=['filename_left'], keep='first', inplace=False, ignore_index=False)
test = test.drop_duplicates(subset=['filename_right'], keep='first', inplace=False, ignore_index=False)
print('Creating Colocation_Files.csv.. ')
test.to_csv('/Users/wndrsn/Colocation_Files.csv')
print('Created Colocation_Files.csv!')
# Extract unique values from the DataFrame columns as file patterns
file_patterns = test['filename_right'].tolist() + test['filename_left'].tolist()

duplicate_h5_and_hdf_files(source_folder_path, destination_folder_path, file_patterns)
