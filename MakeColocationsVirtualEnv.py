import os
import subprocess
import sys


conda_env_name = "ColocationsEnv"

def main():
    # List of libraries to install
    conda_libraries = ['pandas','numpy','h5py','xarray','shapely','geopandas','dask','tqdm']  # Add more libraries as needed
    #pip_libraries = ['modis-tools']

    conda_install(conda_libraries)
    #pip_install(pip_libraries)

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

def pip_install(pip_libraries):
    subprocess.run([sys.executable, '-m', 'pip', 'install'] + pip_libraries)


main()
