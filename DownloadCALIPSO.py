import os
import subprocess
import shutil

cookiejar = "cookies.txt"
netrc = "netrc.txt"

def finish():
    os.remove(cookiejar)
    os.remove(netrc)

# Add this line to create an empty cookiejar file
open(cookiejar, 'w').close()

def prompt_credentials():
    print("Enter your Earthdata Login or other provider supplied credentials")
    username = input("Username")
    password = input("Password: ")
    with open(netrc, "w") as netrc_file:
        netrc_file.write(f"machine urs.earthdata.nasa.gov login {username} password {password}\n")

def exit_with_error(error_message):
    print("\nUnable to Retrieve Data\n")
    print(error_message)
    print("\nhttps://asdc.larc.nasa.gov/data/CALIPSO/LID_L2_05kmAPro-Standard-V4-20/2020/06/CAL_LID_L2_05kmAPro-Standard-V4-20.2020-06-30T23-20-07ZD.hdf\n")
    exit(1)

def detect_app_approval():
    url = "https://asdc.larc.nasa.gov/data/CALIPSO/LID_L2_05kmAPro-Standard-V4-20/2020/06/CAL_LID_L2_05kmAPro-Standard-V4-20.2020-06-30T23-20-07ZD.hdf"
    cmd = f"curl -s -b {cookiejar} -c {cookiejar} -L --max-redirs 5 --netrc-file {netrc} {url} -w '\\n%{{http_code}}' | tail -1"
    approved = int(subprocess.check_output(cmd, shell=True).decode("utf-8").strip())
    if approved not in {200, 301, 302}:
        exit_with_error("Please ensure that you have authorized the remote application by visiting the link below ")

def setup_auth_curl():
    url = "https://asdc.larc.nasa.gov/data/CALIPSO/LID_L2_05kmAPro-Standard-V4-20/2020/06/CAL_LID_L2_05kmAPro-Standard-V4-20.2020-06-30T23-20-07ZD.hdf"
    status = subprocess.call(["curl", "-s", "-z", "$(date)", "-w", "\\n%{http_code}", url])
    if status not in {200, 304}:
        detect_app_approval()

def setup_auth_wget():
    credentials = subprocess.check_output(["grep", "machine urs.earthdata.nasa.gov", "~/.netrc"]).decode("utf-8")
    if not credentials:
        subprocess.call(["cat", netrc, ">>", "~/.netrc"])

def fetch_urls():
    url = "https://asdc.larc.nasa.gov/data/CALIPSO/LID_L2_05kmAPro-Standard-V4-20/2020/06/CAL_LID_L2_05kmAPro-Standard-V4-20.2020-06-30T23-20-07ZD.hdf"
    if shutil.which("curl"):
        setup_auth_curl()
        cmd = f"curl -f -b {cookiejar} -c {cookiejar} -L --netrc-file {netrc} -g -o {url.split('/')[-1].split('?')[0]} -- {url}"
        subprocess.call(cmd, shell=True)
    elif shutil.which("wget"):
        print("\nWARNING: Can't find curl, use wget instead.")
        print("WARNING: Script may not correctly identify Earthdata Login integrations.\n")
        setup_auth_wget()
        cmd = f"wget --load-cookies {cookiejar} --save-cookies {cookiejar} --output-document {url.split('/')[-1].split('?')[0]} --keep-session-cookies -- {url}"
        subprocess.call(cmd, shell=True)
    else:
        exit_with_error("Error: Could not find a command-line downloader. Please install curl or wget")

if __name__ == "__main__":
    try:
        prompt_credentials()
        fetch_urls()
    finally:
        finish()
