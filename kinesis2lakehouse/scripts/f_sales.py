import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Get Last Query Time
GetLastQueryTime_node1721758904314 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://trinm-lakehouse/config/load_config.json"], "recurse": True}, transformation_ctx="GetLastQueryTime_node1721758904314")

# Script generated for node RAW FACT SALES
RAWFACTSALES_node1721761668536 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://trinm-lakehouse/raw/sales/"], "recurse": True}, transformation_ctx="RAWFACTSALES_node1721761668536")

# Script generated for node DIM PRODUCT
DIMPRODUCT_node1721758873903_df = glueContext.create_data_frame.from_catalog(database="thesis", table_name="d_product")
DIMPRODUCT_node1721758873903 = DynamicFrame.fromDF(DIMPRODUCT_node1721758873903_df, glueContext, "DIMPRODUCT_node1721758873903")

# Script generated for node DIM BU
DIMBU_node1721758855929_df = glueContext.create_data_frame.from_catalog(database="thesis", table_name="d_bu")
DIMBU_node1721758855929 = DynamicFrame.fromDF(DIMBU_node1721758855929_df, glueContext, "DIMBU_node1721758855929")

# Script generated for node Read incremental data
SqlQuery0 = '''
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
from f_sales_raw
where event_timestamp >= 
(select 
sales.fact_sales.last_read
from cfg)
'''
Readincrementaldata_node1721758979587 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cfg":GetLastQueryTime_node1721758904314, "f_sales_raw":RAWFACTSALES_node1721761668536}, transformation_ctx = "Readincrementaldata_node1721758979587")

# Script generated for node Final Transform
SqlQuery1 = '''
select cast(row as long) id
       ,cast(d_product.id as int) product_id
       ,cast(d_bu.id as int) bu_id
       ,cast(yr as int) year
       ,cast(mth as int) month
       ,cast(qty as long) qty
       ,cast(coalesce(revenue.int, revenue.long) as long) revenue
       ,cast(event_timestamp as timestamp) event_timestamp
from f_sales
left join d_product
    on f_sales.product_name = d_product.product_name
    and f_sales.price_class = d_product.price_class
    and f_sales.brand_lv1 = d_product.brand_lv1
    and f_sales.brand_lv2 = d_product.brand_lv2
left join d_bu
 on f_sales.business_unit = d_bu.business_unit
 and f_sales.province_lv1 = d_bu.province_lv1
 and f_sales.province_lv2 = d_bu.province_lv2
'''
FinalTransform_node1721759080308 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"f_sales":Readincrementaldata_node1721758979587, "d_bu":DIMBU_node1721758855929, "d_product":DIMPRODUCT_node1721758873903}, transformation_ctx = "FinalTransform_node1721759080308")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721759251875_df = FinalTransform_node1721759080308.toDF()
AWSGlueDataCatalog_node1721759251875 = glueContext.write_data_frame.from_catalog(frame=AWSGlueDataCatalog_node1721759251875_df, database="thesis", table_name="f_sales", additional_options={"partitionKeys": ["year", "month"], "write.parquet.compression-codec": "gzip"})

job.commit()