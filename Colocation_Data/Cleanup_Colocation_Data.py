import os
import glob
import pandas as pd
from tqdm import tqdm
# Specify the directory where your files are located
path = '/nfsscratch/Users/wndrsn'
filetype = '.csv'
search_text = 'colocations week '
# List all files in the directory
all_files = glob.glob(os.path.join(path, f'**/*{filetype}'), recursive=True)

# Filter files with 'colocations day' in the name and with .csv extension
filtered_files = [file for file in tqdm(all_files) if search_text in file.lower() and file.endswith(filetype)]
df_lists = []
for file in tqdm(filtered_files):
    try:
        df_lists.append(pd.read_csv(file))
    except Exception as e:
        print(f'error processing {file} due to {e}')
df_lists = pd.concat(df_lists, ignore_index= True)
df_lists.to_excel('/Users/wndrsn/colocationsListFinal.xlsx')
