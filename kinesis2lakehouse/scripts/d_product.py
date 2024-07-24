import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node RAW JSON
RAWJSON_node1721757499754 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://trinm-lakehouse/raw/sales/"], "recurse": True}, transformation_ctx="RAWJSON_node1721757499754")

# Script generated for node TGT DIM PRODUCT
TGTDIMPRODUCT_node1721757531703_df = glueContext.create_data_frame.from_catalog(database="thesis", table_name="d_product")
TGTDIMPRODUCT_node1721757531703 = DynamicFrame.fromDF(TGTDIMPRODUCT_node1721757531703_df, glueContext, "TGTDIMPRODUCT_node1721757531703")

# Script generated for node Last Query Time
LastQueryTime_node1721757547827 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://trinm-lakehouse/config/load_config.json"], "recurse": True}, transformation_ctx="LastQueryTime_node1721757547827")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1721757578621 = ApplyMapping.apply(frame=TGTDIMPRODUCT_node1721757531703, mappings=[("id", "int", "id", "int"), ("product_name", "string", "right_product_name", "string"), ("price_class", "string", "right_price_class", "string"), ("brand_lv1", "string", "right_brand_lv1", "string"), ("brand_lv2", "string", "right_brand_lv2", "string")], transformation_ctx="RenamedkeysforJoin_node1721757578621")

# Script generated for node Delta Load
SqlQuery0 = '''
select distinct
    product_name,
    price_class,
    brand_lv1,
    brand_lv2
from
    myDataSource
where
    event_timestamp >= (
        select
            sales.dim_product.last_read
        from
            lstQ
    )
'''
DeltaLoad_node1721757622973 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":RAWJSON_node1721757499754, "lstQ":LastQueryTime_node1721757547827}, transformation_ctx = "DeltaLoad_node1721757622973")

# Script generated for node Join
DeltaLoad_node1721757622973DF = DeltaLoad_node1721757622973.toDF()
RenamedkeysforJoin_node1721757578621DF = RenamedkeysforJoin_node1721757578621.toDF()
Join_node1721757661298 = DynamicFrame.fromDF(DeltaLoad_node1721757622973DF.join(RenamedkeysforJoin_node1721757578621DF, (DeltaLoad_node1721757622973DF['product_name'] == RenamedkeysforJoin_node1721757578621DF['right_product_name']) & (DeltaLoad_node1721757622973DF['price_class'] == RenamedkeysforJoin_node1721757578621DF['right_price_class']) & (DeltaLoad_node1721757622973DF['brand_lv1'] == RenamedkeysforJoin_node1721757578621DF['right_brand_lv1']) & (DeltaLoad_node1721757622973DF['brand_lv2'] == RenamedkeysforJoin_node1721757578621DF['right_brand_lv2']), "leftanti"), glueContext, "Join_node1721757661298")

# Script generated for node Get data with incremental ID
SqlQuery1 = '''
select
    ROW_NUMBER() OVER (
        ORDER BY
            product_name
    ) + (
        SELECT
            ifnull (max(id), 0)
        FROM
            mid
    ) id,
    product_name,
    price_class,
    brand_lv1,
    brand_lv2
from
    src
'''
GetdatawithincrementalID_node1721757723949 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"src":Join_node1721757661298, "mid":TGTDIMPRODUCT_node1721757531703}, transformation_ctx = "GetdatawithincrementalID_node1721757723949")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721757749399_df = GetdatawithincrementalID_node1721757723949.toDF()
AWSGlueDataCatalog_node1721757749399 = glueContext.write_data_frame.from_catalog(frame=AWSGlueDataCatalog_node1721757749399_df, database="thesis", table_name="d_product", additional_options={})

job.commit()