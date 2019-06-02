# Data Modeling for Sparkify : Song Play Analysis

    Sparkify collects data on songs and user activity on their new music streaming app. The Sparkify analytics team wants to understand what songs users are listening to. The Data modeling for Sparkify makes it easier for their analytics team to query their data. JSON logs on user activity and JSON metadata of the songs on the app now reside in an AWS S3 bucket. Cloud (AWS) is used for storing the data because of huge number of users and the data generation getting larger day by day.
     
    ETL pipeline is built in such a way that extracts Sparkify data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow Sparkify analytics team to continue finding insights in what songs their users are listening to.
      
      Star Schema data model is used for song play analysis since its very optimal for data analytics because the data in dimensional tables are denormalised and does not need of any join operations on them. Also for the analytics to understand what songs the users will be listening to, aggregation will done. So for large amounts of data Star schema will be optimal.
    
> Below are the Fact and Dimensional tables used for the Star Schema.
>
> The Sparkify analysis team specifically want to do analysis on Song Play analysis. Hence in this star schema Fact Table will be songplays. And users, songs, artists, time  are the dimensions for songplays fact table.

###### Fact Table:
    1. SongPlays: records in log data associated with song plays
        (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

###### Dimension Tables
    1. Users: Users in the app
        (user_id, first_name, last_name, gender, level)
    2. Songs: Songs in music database
        (song_id, title, artist_id, year, duration)
    3. Artists: Artists in music database
        (artist_id, name, location, lattitude, longitude)
    4. Time: timestamps of records in songplays broken down into specific units
        (start_time, hour, day, week, month, year, weekday)

###### Source Datasets:
Two datasets namely, Log data and song data both reside in below S3 buckets:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data


###### How to Run:
1. In AWS, create an IAM role and attach Amazon S3 Read Access policy with this role. 
2. Also in AWS, create a S3 bucket for storing the processed output data. 
3. In dl.cfg: Fill up the Access Key ID and Secret Access key in the config file.
4. In etl.py: In main function fill up the 'output_data' variable with your newly created S3 bucket name.
5. RUN etl.py script. This script implements the ETL pipeline. Extracts from S3 bucket, processes it using spark and Loads the output back to Amazon S3.
