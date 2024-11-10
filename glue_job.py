import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Daily_src_raw_data_from_s3
Daily_src_raw_data_from_s3_node1731244842284 = glueContext.create_dynamic_frame.from_catalog(database="airline_db", table_name="daily_raw", transformation_ctx="Daily_src_raw_data_from_s3_node1731244842284")

# Script generated for node Dimension_data_redshift
Dimension_data_redshift_node1731244958860 = glueContext.create_dynamic_frame.from_catalog(database="airline_db", table_name="dev_airline_airline_dim",redshift_tmp_dir="s3://airline-data1/temp/",additional_options={"aws_iam_role": "arn:aws:iam::209479286927:role/redshift123"}, transformation_ctx="Dimension_data_redshift_node1731244958860")

# Script generated for node Join
Join_node1731245377881 = Join.apply(frame1=Dimension_data_redshift_node1731244958860, frame2=Daily_src_raw_data_from_s3_node1731244842284, keys1=["airport_id"], keys2=["originairportid"], transformation_ctx="Join_node1731245377881")

# Script generated for node dep_airport_schema_changes
dep_airport_schema_changes_node1731245801941 = ApplyMapping.apply(frame=Join_node1731245377881, mappings=[("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string"), ("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint")], transformation_ctx="dep_airport_schema_changes_node1731245801941")

# Script generated for node Join
Join_node1731246162475 = Join.apply(frame1=dep_airport_schema_changes_node1731245801941, frame2=Dimension_data_redshift_node1731244958860, keys1=["destairportid"], keys2=["airport_id"], transformation_ctx="Join_node1731246162475")

# Script generated for node Change Schema
ChangeSchema_node1731246198410 = ApplyMapping.apply(frame=Join_node1731246162475, mappings=[("dep_city", "string", "dep_city", "string"), ("dep_airport", "string", "dep_airport", "string"), ("dep_state", "string", "dep_state", "string"), ("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("dep_delay", "bigint", "dep_delay", "long"), ("arr_delay", "bigint", "arr_delay", "long"), ("airport_id", "long", "airport_id", "long"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("state", "string", "arr_state", "string")], transformation_ctx="ChangeSchema_node1731246198410")

# Script generated for node redshift_fact_table
redshift_fact_table_node1731246327943 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1731246198410, database="airline_db", table_name="dev_airline_daily_flights_fact", redshift_tmp_dir="s3://airline-data1/temp/",additional_options={"aws_iam_role": "arn:aws:iam::209479286927:role/redshift123"}, transformation_ctx="redshift_fact_table_node1731246327943")

job.commit()