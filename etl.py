import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

dyf = glueContext.create_dynamic_frame_from_catalog(
    database='capitol-trades',
    table_name='trades'
)

df = dyf.toDF()

split_col = F.split(F.col('politician'), ' ')

df = \
    df.select([F.when(F.col(c) == 'N/A', None).otherwise(F.col(c)).alias(c) for c in df.columns])\
      .withColumn('published_date', F.to_date(F.col('published_date'), 'yyyy dd MMM'))\
      .withColumn('filed_after', F.regexp_replace(F.col('filed_after'), 'days', '').cast('int'))\
      .withColumn('price', F.col('price').cast('float'))\
      .withColumn('traded_date', F.to_date(F.col('traded_date'), 'yyyy dd MMM'))\
      .withColumn('politician_first_name', F.split(F.col('politician'), ' ').getItem(0))\
      .withColumn('politician_last_name', F.split(F.col('politician'), ' ').getItem(1))

output_path = 's3://capitol-trades-data-lake/trades_parquet/'

df.write.mode("overwrite").partitionBy('state').parquet(output_path)
