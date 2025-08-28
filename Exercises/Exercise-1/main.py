import aiohttp
import asyncio
import os
import time
from zipfile import ZipFile
from io import BytesIO

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # bad link
]

CONCURRENT_DOWNLOADS = 6  # limit


def unzip_from_memory(content, filename, destination="downloads/"):
    """Extract CSV from in-memory zip bytes."""
    with ZipFile(BytesIO(content)) as zf:
        for member in zf.namelist():
            if member.endswith(".csv"):
                zf.extract(member, destination)
                print(f"Extracted {member} from {filename}")


async def download_file(session, url, sem):
    filename = url.split("/")[-1]
    async with sem:  # limit concurrency
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    print(f"✅ Success: {filename}")
                    return content, filename
                else:
                    print(f"❌ Failed ({response.status}): {filename}")
        except Exception as e:
            print(f"⚠️ Error downloading {filename}: {e}")
        return None, filename


async def main():
    start_time = time.time()
    os.makedirs("downloads", exist_ok=True)
    sem = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url, sem) for url in download_uris]
        results = await asyncio.gather(*tasks)

        file_tasks = [asyncio.to_thread(
            unzip_from_memory, content, filename) for content, filename in results if content]
        await asyncio.gather(*file_tasks)

    elapsed = time.time() - start_time
    print(f"⏱ Done in {elapsed:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
