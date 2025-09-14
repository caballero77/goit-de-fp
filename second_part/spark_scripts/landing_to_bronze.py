from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, avg, current_timestamp
from pyspark.sql.types import StringType
import os
import requests
import sys

def download_data(file):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + file + ".csv"
    print(f"Downloading from {downloading_url}")
    
    response = requests.get(downloading_url)
    if response.status_code == 200:
        with open(file + ".csv", "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {file}")
    else:
        raise Exception(f"Failed to download the file. Status code: {response.status_code}")

def main():
    spark = SparkSession.builder.appName("LandingToBronze").master("local").getOrCreate()
    tables = ["athlete_bio", "athlete_event_results"]
    
    for table in tables:
        local_path = f"{table}.csv"
        download_data(table)
        df = spark.read.csv(local_path, header=True, inferSchema=True)
        output_path = f"/tmp/bronze/{table}"
        os.makedirs(output_path, exist_ok=True)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Data saved to {output_path}")
        df = spark.read.parquet(output_path)
        df.show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()