import sys
from pyspark.sql.dataframe import DataFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

STORAGE = "s3"
DB_NAME = "thesis"
CONFIG_TABLE = "cfg"
D_BU = "d_bu"
D_PRODUCT = "d_product"
F_SALES_RAW = "f_sales_raw"
F_SALES = "f_sales"
FACT_PARTITION_BY = ["year", "month"]

# Load dimensions
df_dimproduct = glueContext.create_data_frame.from_catalog(
    database=DB_NAME, 
    table_name=D_PRODUCT
)
dynf_dproduct: DynamicFrame = DynamicFrame.fromDF(df_dimproduct, glue_ctx=glueContext)
# print(df_dimproduct.count())
dynf_dproduct.toDF().createOrReplaceTempView(D_PRODUCT)

df_dimbu = glueContext.create_data_frame.from_catalog(
    database=DB_NAME, 
    table_name=D_BU
)
dynf_dbu: DynamicFrame = DynamicFrame.fromDF(df_dimbu, glue_ctx=glueContext)
# print(df_dimbu.count())
dynf_dbu.toDF().createOrReplaceTempView(D_BU)

# Load config files
df_config: DataFrame = glueContext.create_data_frame.from_options(
    format_options={"multiline": False}, 
    connection_type=STORAGE, 
    format="json", 
    connection_options={
        "paths": ["s3://trinm-lakehouse/config/load_config.json"]
    }, 
)
# print(df_config.count())
df_config.createOrReplaceTempView(CONFIG_TABLE)

# Load facts
df_factsales_raw: DataFrame = glueContext.create_data_frame.from_options(
    format_options={"multiline": False}, 
    connection_type=STORAGE, 
    format="json", 
    connection_options={
        "paths": ["s3://trinm-lakehouse/raw/sales/"], 
        "recurse": True
    }
)
# print(df_factsales_raw.count())
df_factsales_raw.createOrReplaceTempView(F_SALES_RAW)

# Read incremental data
incrementalLoadQuery = f"""
select row
       ,yr
       ,mth
       ,business_unit
       ,province_lv1
       ,province_lv2
       ,product_name
       ,brand_lv1
       ,brand_lv2
       ,price_class
       ,qty
       ,revenue
       ,event_timestamp
from {F_SALES_RAW}
where event_timestamp >= 
(select 
sales.fact_sales.last_read
from {CONFIG_TABLE})
"""
df_factsales = spark.sql(incrementalLoadQuery)
# print(df_factsales.count())
df_factsales.createOrReplaceTempView(F_SALES)

# Transform
transformFactQuery = f"""
select cast(row as long) id
       ,cast({D_PRODUCT}.id as int) product_id
       ,cast({D_BU}.id as int) bu_id
       ,cast(yr as int) year
       ,cast(mth as int) month
       ,cast(qty as long) qty
       ,cast(coalesce(revenue.int, revenue.long) as long) revenue
       ,cast(event_timestamp as timestamp) event_timestamp
from {F_SALES}
left join {D_PRODUCT}
    on {F_SALES}.product_name = {D_PRODUCT}.product_name
    and {F_SALES}.price_class = {D_PRODUCT}.price_class
    and {F_SALES}.brand_lv1 = {D_PRODUCT}.brand_lv1
    and {F_SALES}.brand_lv2 = {D_PRODUCT}.brand_lv2
left join {D_BU}
 on {F_SALES}.business_unit = {D_BU}.business_unit
 and {F_SALES}.province_lv1 = {D_BU}.province_lv1
 and {F_SALES}.province_lv2 = {D_BU}.province_lv2
"""
df_factsales_cleaned = spark.sql(transformFactQuery)
print(df_factsales_cleaned.count())

# Write to S3
additional_options = {"write.parquet.compression-codec": "gzip"}
tables_collection = spark.catalog.listTables(DB_NAME)
table_names_in_db = [table.name for table in tables_collection]
table_exists = F_SALES in table_names_in_db

if table_exists:
    df_factsales_cleaned.sortWithinPartitions('year', 'month') \
        .writeTo(f"glue_catalog.{DB_NAME}.{F_SALES}") \
        .tableProperty("format-version", "2") \
        .tableProperty("location", "s3://trinm-lakehouse/silver/{F_SALES}") \
        .options(**additional_options) \
        .partitionedBy('year', 'month').append()
else:
    df_factsales_cleaned.sortWithinPartitions('year', 'month') \
        .writeTo(f"glue_catalog.{DB_NAME}.{F_SALES}") \
        .tableProperty("format-version", "2") \
        .tableProperty("location", "s3://trinm-lakehouse/silver/{DB_NAME}/{F_SALES}") \
        .options(**additional_options) \
        .partitionedBy('year', 'month').create()

job.commit()