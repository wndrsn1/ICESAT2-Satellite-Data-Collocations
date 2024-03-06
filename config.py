import os
import subprocess
import sys


conda_env_name = "CollocationsEnv"

def main():
    conda_libraries = ['pandas','numpy','h5py','xarray','shapely','geopandas','dask','tqdm','plotly']  # Add more libraries as needed
    conda_install(conda_libraries)


def conda_install(conda_libraries):
    commands = [
        f'conda activate {conda_env_name}',
        f'conda install -n {conda_env_name} {" ".join(conda_libraries)} --yes',  # Install libraries
        'conda list'
    ]
    # Execute each command using conda run
    os.system(f'conda create -n {conda_env_name} --yes')
    for command in commands:
        os.system(f'conda run -n {conda_env_name} {command}')


main()
