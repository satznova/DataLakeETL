import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id,to_timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS_ACCESS_KEYS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS_ACCESS_KEYS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
     """ The function creates a new Spark session object. 
  
        Parameters: Nothing
          
        Returns: spark session object
     """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_data(spark, input_data, output_data):
     """ The function extracts data from S3 and does processing to create Fact and Dim tables. 
         The processed output is presisted back in S3 buckets as Parquet files. 
  
        Parameters: 
            spark: Spark session object
            input_data: Input S3 Bucket
            output_data: Output S3 Bucket
            
        Returns: Nothing
     """
    # FILE PATHS: song and log data files
    song_data = input_data + "song-data/*/*/*/*.json"
    log_data  = input_data + "log_data/*/*/*.json"
    
    # READ: song data file
    df_input_song = spark.read.json(song_data)
    
    # READ: log data file and filter by actions for song plays
    df_input_logs = spark.read.json(log_data)
    df_input_logs = df_input_logs.filter(df_input_logs.page == "NextSong")
   
    
    # FACT TABLE: fact_songplays 
    songs = df_input_song.alias("songs")
    logs  = df_input_logs.alias("logs")
    df_songplays = logs.join(songs, (logs.artist == songs.artist_name) & (logs.song == songs.title)) \
                    .withColumn("songplay_id", monotonically_increasing_id()) \
                    .withColumn("start_time", to_timestamp((logs.ts/1000).cast('timestamp'), "yyyy-MM-dd hh:mm:ss")) \
                    .selectExpr( "songplay_id", \
                                 "start_time", \
                                 "logs.userId AS user_id", \
                                 "logs.level",\
                                 "songs.song_id", \
                                 "songs.artist_id", \
                                 "logs.sessionId AS session_id", \
                                 "songs.artist_location AS location", \
                                 "logs.userId AS user_agent", \
                                 "year(CAST(start_time AS date)) AS year", \
                                 "month(CAST(start_time AS date)) AS month")   # year & month column used for partitioning
    
    # DIM TABLE 1: dim_songs
    df_songs = df_input_song.selectExpr("song_id",\
                                        "title", \
                                        "artist_id", \
                                        "year", \
                                        "duration")

    # DIM TABLE 2: dim_artists
    df_artists = df_input_song.filter("artist_id is not null and artist_id != ''") \
                               .selectExpr("artist_id",\
                                           "artist_name AS name",\
                                           "artist_location AS location",\
                                           "artist_latitude AS latitude",\
                                           "artist_longitude AS longitude")
    
    # DIM TABLE 3: dim_users
    df_users = df_input_logs.filter("userId is not null and userId != ''") \
                            .selectExpr("userId AS user_id", \
                                        "firstName AS first_name", \
                                        "lastName AS last_name", \
                                        "gender", \
                                        "level")
    
     # DIM TABLE 4: dim_time
    df_time = df_songplays.withColumn("hour" , hour(df_songplays.start_time)) \
                           .withColumn("day", dayofmonth(df_songplays.start_time)) \
                           .withColumn("week", weekofyear(df_songplays.start_time)) \
                           .withColumn("month", month(df_songplays.start_time)) \
                           .withColumn("year", year(df_songplays.start_time)) \
                           .withColumn("weekday", date_format(df_songplays.start_time, 'EEEE')) \
                           .select('start_time', 'hour', 'week', 'month', 'year', 'weekday')
    
    # write: as Parquet files
    #df_users.write.parquet("output/dim_users.parquet")
    #df_artists.write.parquet("output/dim_artists.parquet")
    #df_songs.write.partitionBy('year','artist_id').parquet("output/dim_songs.parquet")
    #df_time.write.partitionBy('year','month').parquet("output/dim_time.parquet")
    #df_songplays.write.partitionBy('year','month').parquet("output/fact_songplays.parquet")   
    df_songplays.write.partitionBy('year','month').parquet(output_data + "fact_songplays.parquet")
    df_users.write.parquet(output_data + "dim_users.parquet")
    df_artists.write.parquet(output_data + "dim_artists.parquet")
    df_songs.write.partitionBy('year','artist_id').parquet(output_data + "dim_songs.parquet")
    df_time.write.partitionBy('year','month').parquet(output_data + "dim_time.parquet")    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-output-parquet/"
    
    process_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
