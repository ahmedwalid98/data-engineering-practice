import requests
import os
from zipfile import ZipFile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    # your code
    os.makedirs('downloads', exist_ok=True)
    for url in download_uris:

        file_name = url.split('/')[-1]
        response = requests.get(url)
        if response.status_code == 200:
            print(f'Successed to download {url}')
            with open(f'./downloads/{file_name}', 'wb') as f:
                f.write(response.content)

            with ZipFile(f'./downloads/{file_name}', 'r') as zobj:
                zobj.extract(file_name.replace('.zip', '.csv'), './downloads/')
                zobj.close()

            os.remove(f'./downloads/{file_name}')
        else:
            print(f'Failed to download {url}')


if __name__ == "__main__":
    main()
