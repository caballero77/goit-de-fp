from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_extract, when, isnan, isnull
from pyspark.sql.types import StringType, DoubleType
import os

def clean_text(df):
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, trim(lower(col(column))))
    return df

def process_weight(df):
    """
    Some values of weight are formated like 130-141
    We need to extract the average value of the range
    """
    if 'weight' in df.columns:
        df = df.withColumn(
            'weight_processed',
            when(
                col('weight').rlike(r'^\d+-\d+$'),
                (regexp_extract(col('weight'), r'^(\d+)-', 1).cast(DoubleType()) + 
                regexp_extract(col('weight'), r'-(\d+)$', 1).cast(DoubleType())) / 2
            ).when(
                col('weight').rlike(r'^\d+\.?\d*$'),
                col('weight').cast(DoubleType())
            ).otherwise(
                None
            )
        ).drop('weight').withColumnRenamed('weight_processed', 'weight')
    
    return df

def main():
    spark = SparkSession.builder.appName("BronzeToSilver").master("local").getOrCreate()
    tables = ["athlete_bio", "athlete_event_results"]
    
    for table in tables:
        df = spark.read.parquet(f"/tmp/bronze/{table}")
        df = clean_text(df)
        df = process_weight(df)
        df = df.dropDuplicates()
        output_path = f"/tmp/silver/{table}"
        os.makedirs(output_path, exist_ok=True)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Data saved to {output_path}")
        df = spark.read.parquet(output_path)
        df.show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()