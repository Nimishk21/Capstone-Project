# Do all imports and installs here
import configparser
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import first,split
from pyspark.sql.functions import upper, col
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from pyspark.sql.functions import udf, date_format
import datetime as dt
from pyspark.sql.functions import col, countDistinct
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


spark = SparkSession.builder.\
config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
.getOrCreate()


# Read the demographic data here
us_demo=spark.read.csv("./us-cities-demographics.csv", sep=';', header=True)
us_demo.columns

# Creating new columns from Race using Pivot and aggregating using count
us_race_cnt=(us_demo.select("city","state code","Race","count")
    .groupby(us_demo.City, "state code")
    .pivot("Race")
    .agg(first("Count")))

# Drop columns we don't need and drop duplicate rows
uscols=["Number of Veterans","Race","Count"]
us_demo=us_demo.drop(*uscols).dropDuplicates()

# Joining us_demo and us_race-cnt on common city,state and code
us_demo=us_demo.join(us_race_cnt, ["city","state code"])

# Change `state code` column name to `state_code` and other similar problems to avoid parquet complications
us_demo=us_demo.select('City', col('State Code').alias('State_Code'), 'State', col('Median Age').alias('Median_age'),
     col('Male Population').alias('Male_Population'), col('Female Population').alias('Female_Population'), 
        col('Total Population').alias('Total_Population'), 'Foreign-born', 
          col('Average Household Size').alias('Average_Household_Size'),
             col('American Indian and Alaska Native').alias('American_Indian_and_Alaska_Native_Pop'), 
                 col('Asian').alias('Asian_Pop'), 
                    col('Black or African-American').alias('Black_or_AfricanAmerican_Pop'), 
                      col('Hispanic or Latino').alias('Hispanic_or_Latino_Pop'), 
                        col('White').alias('White_Pop'))

# Drop the state column
us_demo=us_demo.drop("state")

# Now write transformed `US_demo` dataset onto parquet file(In data folder)
us_demo.write.mode('overwrite').parquet("s3a://my-data-engineer-capstone/us_cities_demographics.parquet")

# We will transform I94_SAS-Labels_Description into I-94ADDR.csv,I94CIT_I94RES.csv,I94MODE.csv,I94Visa.csv and finally saving them into respective parquet files.

# ETL I94Mode 
# Create i94mode list
i94mode=spark.read.csv('./I94MODE.csv',header=True)

# Create i94mode parquet file
i94mode.write.mode("overwrite").parquet('s3a://my-data-engineer-capstone/i94mode.parquet')

# ETL I94Port 
# Read i94port CSV file
i94port= pd.read_csv('./I94PORT.csv')
new=i94port['Port'].str.split(",",expand=True)
i94port['Port_city']=new[0]
i94port['Port_state']=new[1]
i94port.drop(columns=['Port'],inplace=True)
i94port.head()

# Now convert pd dataframe to spark dataframe
# Create a schema for the dataframe
i94port_schema = StructType([
    StructField('id', StringType(), True),
    StructField('port_city', StringType(), True),
    StructField('port_state', StringType(), True)
])
i94port=spark.createDataFrame(i94port, i94port_schema)

# Create i94port parquet file
i94port.write.mode('overwrite').parquet('s3a://my-data-engineer-capstone/i94port.parquet')

# ETL I94Res
#Read I94CIT_I94RES.csv 
i94res=spark.read.csv('./I94CIT_I94RES.csv',header=True)
i94res=i94res.withColumnRenamed('I94CTRY','country')
i94res.show()
# Create i94parquet file
i94res.write.mode('overwrite').parquet('s3a://my-data-engineer-capstone/i94res.parquet')

# ETL I94Visa
#read I94 Visa
i94visa=spark.read.csv('./I94VISA.csv',header=True)
i94visa.columns
# Create parquet file
i94visa.write.mode('overwrite').parquet('s3a://my-data-engineer-capstone/i94visa.parquet')

# Start ETL of i94 non-immigration dataset :

# Read i94 non-immigration dataset
i94_immigration=spark.read.parquet("sas_data")

#Convert columns reads as string/doublr to integer
i94_immigration=i94_immigration.select(col("i94res").cast(IntegerType()),col("i94port"),\
                           col("cicid").cast(IntegerType()),
                           col("arrdate").cast(IntegerType()), \
                           col("i94mode").cast(IntegerType()),col("depdate").cast(IntegerType()),
                           col("i94bir").cast(IntegerType()),col("i94visa").cast(IntegerType()), 
                           col("count").cast(IntegerType()), \
                              "gender","fltno")

# Add i94port city and state columns to i94 dataframe
i94_immigration=i94_immigration.join(i94port, i94_immigration.i94port==i94port.id, how='left')

# Join US with i94_spark to get fact table `i94non_immigrant_port_entry`
# NOTE: We use left join againt city records which may cause null values because
# we may not currently have demographic stats on all U.S. ports of entry
i94_immigration=i94_immigration.join(us_demo, (upper(i94_immigration.port_city)==upper(us_demo.City)) & \
                                           (upper(i94_immigration.port_state)==upper(us_demo.State_Code)), how='left')

# Convert SAS arrival date to datetime format
get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
i94_immigration= i94_immigration.withColumn("arrival_date", get_date(i94_immigration.arrdate))

#renaming 'i94res','i94port','i94mode','i94visa'
i94_immigration=i94_immigration.withColumnRenamed('i94res','res_id')\
                               .withColumnRenamed('i94port','port_id')\
                               .withColumnRenamed('i94mode','mode_id')\
                               .withColumnRenamed('i94visa','visa_id')

# Create parquet file
i94_immigration.drop('arrdate').write.mode("overwrite").parquet('s3a://my-data-engineer-capstone/i94_immigration.parquet')


# Date ETL
i94date=i94_immigration.select(
                                col('arrival_date'),
                                date_format('arrival_date','M').alias('arrival_month'),
                                date_format('arrival_date','E').alias('arrival_dayofweek'), 
                                date_format('arrival_date', 'y').alias('arrival_year'), 
                                date_format('arrival_date', 'd').alias('arrival_day'),
                                date_format('arrival_date','w').alias('arrival_weekofyear')).dropDuplicates()
# Create parquet file
i94date.write.mode("overwrite").partitionBy("arrival_year", "arrival_month").parquet('s3a://my-data-engineer-capstone/i94date.parquet')