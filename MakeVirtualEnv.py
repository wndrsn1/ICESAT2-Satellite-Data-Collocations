import os

# Define the Conda environment name
conda_env_name = "willstest"

# List of libraries to install
libraries = ["numpy", "pandas"]  # Add more libraries as needed

# Commands to create and activate a Conda environment
commands = [
    f'conda activate {conda_env_name}',
    f'conda install -n {conda_env_name} {" ".join(libraries)} --yes',  # Install libraries
    'conda list'
]

# Execute each command using conda run
os.system(f'conda create -n {conda_env_name} --yes')
for command in commands:
    os.system(f'conda run -n {conda_env_name} {command}')
