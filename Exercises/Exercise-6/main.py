from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import expr, col, to_date, date_sub, unix_timestamp, to_timestamp, avg, count, month, max, dense_rank, lit
from pyspark.sql.types import LongType
from glob import glob
from zipfile import ZipFile

file_names = glob('./data/*.zip')


def read_file_from_zip(path):
    filename = path.split('\\')[-1].split('.')[0]
    with ZipFile(path, 'r') as zf:
        zf.extractall('./data/')
    return filename


def create_spark_df(sc, path):
    return sc.read.csv(path, header=True, inferSchema=True)


def get_spark_df(sc):

    df = sc.select("*")

    if "started_at" in df.columns:  # 2020_Q1 schema
        df = df.withColumnRenamed("started_at", "start_time") \
               .withColumnRenamed("ended_at", "end_time") \
               .withColumnRenamed("start_station_id", "from_station_id") \
               .withColumnRenamed("start_station_name", "from_station_name")

        df = df.withColumn(
            "tripduration",
            (unix_timestamp(to_timestamp(col("end_time"))) -
             unix_timestamp(to_timestamp(col("start_time")))).cast(LongType())
        )
    else:
        df = df.withColumn("tripduration", col(
            "tripduration").cast(LongType()))

    df = df.withColumn("date", to_date(col("start_time")))

    return df


def get_average_trip(df):
    return df.groupby('date').agg(avg('tripduration').alias('avg_duration'))


def get_trips_count(df):
    return df.groupby('date').agg(count('*').alias('trip_count'))


def get_most_trip_station(df):
    df = df.withColumn('month', month(col('date')))
    count_trips_df = df.groupby('from_station_name', 'month').agg(
        count('*').alias('trip_counts'))
    window = Window.orderBy(col('trip_counts').desc()).partitionBy('month')
    trips_rank = count_trips_df.withColumn('drank', dense_rank().over(window))
    return trips_rank.filter(col('drank') == 1).drop('drank')


def get_most_three_trips_last_two_weeks(df):
    max_date = df.select(max('date').alias(
        'max_date'))
    last_two_weeks_df = df.join(max_date).filter(
        col("date").between(date_sub(col("max_date"), 14), col("max_date"))
    ).drop(col('max_date'))

    last_two_weeks_df = last_two_weeks_df.groupby(
        'from_station_name', 'date').agg(count('*').alias('station_count'))
    window = Window.orderBy(col('station_count').desc()).partitionBy('date')
    last_two_weeks_df = last_two_weeks_df.withColumn(
        'drank', dense_rank().over(window))
    return last_two_weeks_df.filter(col('drank').between(1, 3)).drop('drank')


def main():
    spark = SparkSession.builder.appName(
        "Exercise6").enableHiveSupport().getOrCreate()

    for path in file_names:
        files = read_file_from_zip(path)
        df = create_spark_df(spark, f'./data/{files}.csv')
        trip_df = get_spark_df(df)
        trip_df.show(5)
        average_trip_df = get_average_trip(trip_df)
        average_trip_df.show(5)
        trip_count_df = get_trips_count(trip_df)
        trip_count_df.show(5)
        most_trip_station = get_most_trip_station(trip_df)
        most_trip_station.show(5)
        get_most_three_trips_last_two_weeks(trip_df).show(5)


if __name__ == "__main__":
    main()
