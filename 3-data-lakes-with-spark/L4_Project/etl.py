import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates the Spark Session
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function process all the songs data from the json files
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (
        df.select(
            'song_id', 'title', 'artist_id',
            'year', 'duration'
        ).distinct()
    )
    
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    # write songs table to parquet files partitioned by year and artist
    #songs_table.write.parquet(output_data + "songs.parquet", mode="overwrite")
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = (
        df.select(
            'artist_id',
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('latitude'),
            col('artist_longitude').alias('longitude'),
        ).distinct()
    )
    
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    # write artists table to parquet files
    #artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    


def process_log_data(spark, input_data, output_data):
    """
    This function process all event logs of the Sparkify app.
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = (
        df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
    )
    
    users_table = users_table.orderBy("ts",ascending=False).dropDuplicates(subset=["userId"]).drop('ts')
    # write users table to parquet files
    #users_table.write.parquet(output_data + "users.parquet", mode="overwrite")
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    #df =
    df = df.withColumn(
        "ts_timestamp",
        F.to_timestamp(F.from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp")
    )

    def get_weekday(date):
        """
        This function gets weekday from date
        :param date:
        :return weekday:
        """
        import datetime
        import calendar
        date = date.strftime("%m-%d-%Y")  # , %H:%M:%S
        month, day, year = (int(x) for x in date.split('-'))
        weekday = datetime.date(year, month, day)
        return calendar.day_name[weekday.weekday()]

    udf_week_day = udf(get_weekday, T.StringType())
    
    # extract columns to create time table
    time_table = (
        df.withColumn("hour", hour(col("ts_timestamp")))
          .withColumn("day", dayofmonth(col("ts_timestamp")))
          .withColumn("week", weekofyear(col("ts_timestamp")))
          .withColumn("month", month(col("ts_timestamp")))
          .withColumn("year", year(col("ts_timestamp")))
          .withColumn("weekday", udf_week_day(col("ts_timestamp")))
          .select(
            col("ts_timestamp").alias("start_time"),
            col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            col("weekday")
          ).distinct()
    )
    
    time_table = time_table.drop_duplicates(subset=['start_time'])
    # write time table to parquet files partitioned by year and month
    #time_table.write.parquet(output_data + "time.parquet", mode="overwrite")
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = (
        df.withColumn("songplay_id", F.monotonically_increasing_id())
          .join(song_df, song_df.title == df.song)
          .select(
            "songplay_id",
            col("ts_timestamp").alias("start_time"),
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent")
          ).distinct()
    )

    # write songplays table to parquet files partitioned by year and month
    #songplays_table.write.parquet(output_data + "songplays.parquet", mode="overwrite")
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()