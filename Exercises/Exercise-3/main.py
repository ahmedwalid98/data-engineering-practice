import boto3
import gzip


def main():
    s3_client = boto3.client("s3")

    s3_bucket = "commoncrawl"
    wet_paths_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    obj = s3_client.get_object(Bucket=s3_bucket, Key=wet_paths_key)
    with gzip.GzipFile(fileobj=obj['Body']) as f:

        for line_num, wet_file_uri in enumerate(f):
            wet_file_uri = wet_file_uri.decode("utf-8").strip()
            print(f"Processing {wet_file_uri}")

            wet_obj = s3_client.get_object(Bucket=s3_bucket, Key=wet_file_uri)
            with gzip.GzipFile(fileobj=wet_obj['Body']) as wet_f:
                for line in wet_f:
                    print(line.decode("utf-8").strip())

            if line_num >= 0:  # remove or adjust this to process more files
                break  # only process the first file for testing


if __name__ == "__main__":
    main()
