import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,from_unixtime,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import StructType, StructField, DoubleType , StringType , IntegerType, DateType,LongType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''Return the spark Session object'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def getSchema(table):
    '''
    Return StructType schema object used for loading json data.
    Parameters:
        table: song/log data.
    returns:
        Properly structed spark schema.
    '''
    if table=="song":
        Schema = StructType([
            StructField("num_songs",IntegerType(),True),
            StructField("artist_id",StringType(), True),
            StructField("artist_latitude",DoubleType(),True),
            StructField("artist_longitude",DoubleType(),True),
            StructField("artist_location",StringType(), True),
            StructField("artist_name",StringType(), True),
            StructField("song_id",StringType(), True),
            StructField("title",StringType(), True),
            StructField("duration",DoubleType(),True),
            StructField("year",IntegerType(),True)
        ])
    if table=="log":
        Schema = StructType([StructField("artist",StringType(),True),
                        StructField("auth",StringType(), True),
                        StructField("firstname",StringType(),True),
                        StructField("gender",StringType(),True),
                        StructField("itemInSession",IntegerType(), True),
                        StructField("lastName",StringType(), True),
                        StructField("length",DoubleType(), True),
                        StructField("level",StringType(), True),
                        StructField("location",StringType(),True),
                        StructField("method",StringType(),True),
                        StructField("page",StringType(), True),
                        StructField("registration",StringType(), True),
                        StructField("sessionId",IntegerType(),True),
                        StructField("song",StringType(),True),
                        StructField("status",IntegerType(), True),
                        StructField("ts",LongType(),True),
                        StructField("userAgent",StringType(),True),
                        StructField("userId",StringType(),True)
                        ])
    return Schema

    
def process_song_data(spark, input_data, output_data):
    '''
    Process song/artist json data, write parquet files to S3 or local location. 
    Partition songs table with year and artist_id.
    
    Parameters:
        spark: spark session object
        input_data: S3/local root directory from where pull json data 
        output_data: S3/Local root directory to where put parquet file 
        
    returns:
        song table spark dataframe provide to process_log_data function to use.
    '''
    song_Schema =getSchema("song")
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data,schema=song_Schema)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")\
                    .where(df.song_id.isNotNull())\
                    .distinct()
    
    # write songs table to parquet files partitioned by year and artist
    # Reference: https://knowledge.udacity.com/questions/433205
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.where(df.artist_id.isNotNull())\
                      .select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude")\
                      .distinct()
    artists_table.withColumnRenamed("artist_name","name")\
    .withColumnRenamed("artist_location","location")\
    .withColumnRenamed("artist_latitude","lattitude")\
    .withColumnRenamed("artist_longitude","longitude")\
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))
    return songs_table


def process_log_data(spark, input_data, output_data,songs_table):
    '''
    Process log data to extract time/users/songplay event
    information. Load parquet files to S3/local system.
    
    Parameters:
        spark: spark session object.
        input_data: S3/local root directory from where pull json data 
        output_data: S3/Local root directory to where put parquet file 
        songs_table: input spark dataframe use for joining by songplay table.
    '''
    
        
    log_schema=getSchema("log")
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df =  spark.read.json(log_data,schema=log_schema)
    
    # filter by actions for song plays
    df_user = df.filter(df.page == 'NextSong')\
           .filter(col("userId").cast("int").isNotNull())\
           .select(col("userId").cast("int"),"firstName","lastName","gender","level")

    # extract columns for users table    
    users_table = df_user.where(df.userId.isNotNull())\
                .select("userId","firstName","lastName","gender","level")\
                .selectExpr("userId as user_id",
                            "firstName as first_name",
                            "lastName as last_name"
                            )\
                .distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time',from_unixtime(col("ts")/1000).cast("timestamp"))
    
    # create datetime column from original timestamp column
    df_time = df.select("start_time")\
                .where(df.start_time.isNotNull())\
                .distinct()

    
    # extract columns to create time table
    time_table = df_time.withColumn('hour',hour("start_time"))\
                .withColumn('day',dayofmonth("start_time"))\
                .withColumn('week',weekofyear("start_time"))\
                .withColumn('month',month("start_time"))\
                .withColumn('year',year("start_time"))\
                .withColumn('weekday',dayofweek("start_time"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = songs_table.select("song_id","title","artist_id")
                
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,df.song==song_df.title)\
                               .withColumn("year",year("start_time"))\
                               .withColumn("month",month("start_time"))\
                               .withColumn("songplay_id",monotonically_increasing_id())\
                               .select("songplay_id","start_time","userId","level",
                                        "song_id","artist_id","sessionId","location","useragent","year","month")\
                               .dropDuplicates(['userId', 'start_time','song_id'])\
                               .withColumnRenamed("userId","user_id")\
                               .withColumnRenamed("sessionId","session_id")\
                               .withColumnRenamed("useragent","user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "data"
    
    songs_table=process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data,songs_table)


if __name__ == "__main__":
    main()
