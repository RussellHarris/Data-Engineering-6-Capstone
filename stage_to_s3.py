# Imports and installs
import boto3
import configparser
import os
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['KEY']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['SECRET']
s3_bucket = config.get("S3","BUCKET")
s3_raw_data = 's3a://' + s3_bucket + '/raw'

s3 = boto3.resource('s3', region_name="us-west-2")

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
    .enableHiveSupport().getOrCreate()

# Load I94 Immigration Data to Spark
i94_jan16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat')
i94_feb16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_feb16_sub.sas7bdat')
i94_mar16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_mar16_sub.sas7bdat')
i94_apr16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
i94_may16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_may16_sub.sas7bdat')
i94_jun16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat')
i94_jul16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_jul16_sub.sas7bdat')
i94_aug16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_aug16_sub.sas7bdat')
i94_sep16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_sep16_sub.sas7bdat')
i94_oct16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_oct16_sub.sas7bdat')
i94_nov16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_nov16_sub.sas7bdat')
i94_dec16_sub =spark.read.format('com.github.saurfang.sas.spark').load('/data/18-83510-I94-Data-2016/i94_dec16_sub.sas7bdat')

# Remove extra columns in June 2016 data
columns_to_drop = ['validres','delete_days','delete_mexl','delete_dup','delete_visa','delete_recdup']
i94_jun16_sub = i94_jun16_sub.drop(*columns_to_drop)

df_i94 = [i94_jan16_sub,
          i94_feb16_sub,
          i94_mar16_sub,
          i94_apr16_sub,
          i94_may16_sub,
          i94_jun16_sub,
          i94_jul16_sub,
          i94_aug16_sub,
          i94_sep16_sub,
          i94_oct16_sub,
          i94_nov16_sub,
          i94_dec16_sub]

df_i94_names = ['i94_jan16_sub',
                'i94_feb16_sub',
                'i94_mar16_sub',
                'i94_apr16_sub',
                'i94_may16_sub',
                'i94_jun16_sub',
                'i94_jul16_sub',
                'i94_aug16_sub',
                'i94_sep16_sub',
                'i94_oct16_sub',
                'i94_nov16_sub',
                'i94_dec16_sub']

# Upload I94 Immigration Data to S3
count=0
for i,df in enumerate(df_i94):
    df.write.mode('ignore').parquet(s3_raw_data + '/i94_immigration_data/' + df_i94_names[count])
    print('Upload of ' + df_i94_names[count] + '...complete')
    count += 1
    
# Upload Raw and Lookup data to S3
s3.meta.client.upload_file('/data2/GlobalLandTemperaturesByCity.csv', s3_bucket, 'raw/world_temperature_data/GlobalLandTemperaturesByCity.csv')
s3.meta.client.upload_file('./raw_data/us-cities-demographics.csv', s3_bucket, 'raw/us_city_demographic_data/us-cities-demographics.csv')
s3.meta.client.upload_file('./raw_data/airport-codes_csv.csv', s3_bucket, 'raw/airport_code_data/airport-codes_csv.csv')
s3.meta.client.upload_file('./lookup_data/i94addrl.csv', s3_bucket, 'lookup/i94addrl.csv')
s3.meta.client.upload_file('./lookup_data/i94cntyl.csv', s3_bucket, 'lookup/i94cntyl.csv')
s3.meta.client.upload_file('./lookup_data/i94model.csv', s3_bucket, 'lookup/i94model.csv')
s3.meta.client.upload_file('./lookup_data/i94prtl.csv', s3_bucket, 'lookup/i94prtl.csv')
s3.meta.client.upload_file('./lookup_data/i94prtl_enriched.csv', s3_bucket, 'lookup/i94prtl_enriched.csv')
s3.meta.client.upload_file('./lookup_data/i94visal.csv', s3_bucket, 'lookup/i94visal.csv')
print('Upload of CSV data...complete')

# EOF