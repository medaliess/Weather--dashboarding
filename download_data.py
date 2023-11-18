import requests
import os
from tqdm import tqdm
from io import BytesIO
import gzip



START_YEAR=1949
END_YEAR=2023
BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa/"
TOTAL_FILES = END_YEAR - START_YEAR + 1

# make sure the folder to hold the downloaded files exists
if not os.path.exists("./downloads/SAFI_meteo/"):
    os.makedirs("./downloads/SAFI_meteo/")

# keep track of the downloaded and skipped files
downloaded = 0
skipped = 0

# download the files
print("Downloading files...")
for i in tqdm(range(START_YEAR, END_YEAR+1 )):
    
    # get the file name and link you want to download from the Noaa website
    file_name = f"601850-99999-{i}.gz"
    file_link = f"{BASE_URL}/{i}/{file_name}"


    # if the file already exists, skip
    if os.path.exists(f"./downloads/SAFI_meteo/{file_name}"):
        downloaded += 1
        continue

    response = requests.get(file_link)

    if response.status_code == 200:
        # File exists, proceed with downloading and extraction
        content = BytesIO(response.content)
        
    else:
        # Handle the error, for example:
        print(f" Failed to download file for year {i}. Status Code: {response.status_code}")
        skipped += 1
        continue


    # extract the data
    with gzip.GzipFile(fileobj=content) as f:
        extracted_data = f.read()

    # write to new file
    with open(f"./downloads/SAFI_meteo/{file_name[:-3]}", "wb") as f:
        f.write(extracted_data)

    downloaded += 1

print(
    f"Successfully downloaded {downloaded} files out of {TOTAL_FILES}...")
print(f"Skipped {skipped} files out of {TOTAL_FILES}...")
