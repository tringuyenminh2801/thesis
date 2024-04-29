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

# Script generated for node TGT DIM PRODUCT
df_tgt = glueContext.create_data_frame.from_catalog(
    database="thesis", 
    table_name="d_product"
)
dynf_tgt = DynamicFrame.fromDF(
    df_tgt, 
    glueContext, 
    "TGTDIMPRODUCT_node1713692311534"
)

# Script generated for node Amazon S3
dynf_src_raw = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, 
    connection_type="s3", 
    format="json", 
    connection_options={
        "paths": ["s3://trinm-lakehouse/raw/sales/"], 
        "recurse": True
    }
)

# Script generated for node Last Query Time
dynf_lstQ = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False}, 
    connection_type="s3",
    format="json", 
    connection_options={
        "paths": ["s3://trinm-lakehouse/config/load_config.json"], 
        "recurse": True
    }
)

# Script generated for node Renamed keys for Join
dynf_tgt_renamed = ApplyMapping.apply(
    frame=dynf_tgt, 
    mappings=[("id", "int", "id", "int"), 
              ("product_name", "string", "right_product_name", "string"), 
              ("price_class", "string", "right_price_class", "string"), 
              ("brand_lv1", "string", "right_brand_lv1", "string"), 
              ("brand_lv2", "string", "right_brand_lv2", "string")], 
    transformation_ctx="RenamedkeysforJoin_node1713692723455")

# Script generated for node Delta Load
SqlQuery1 = '''
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
dynf_dltload_src = sparkSqlQuery(
    glueContext, 
    query = SqlQuery1, 
    mapping = {
        "myDataSource" : dynf_src_raw, 
        "lstQ":dynf_lstQ
    }
)

# Script generated for node Join
df_dltload_src = dynf_dltload_src.toDF()
df_forjoin = dynf_tgt_renamed.toDF()
df_join = DynamicFrame.fromDF(
    df_dltload_src.join(
        df_forjoin, 
        (df_dltload_src['product_name'] == df_forjoin['right_product_name']) 
        & (df_dltload_src['price_class'] == df_forjoin['right_price_class']) 
        & (df_dltload_src['brand_lv1'] == df_forjoin['right_brand_lv1']) 
        & (df_dltload_src['brand_lv2'] == df_forjoin['right_brand_lv2']), 
        "leftanti"
    ), 
    glueContext
)

# Script generated for node Get data with incremental ID
genIDQuery = '''
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
df_genID = sparkSqlQuery(
    glueContext, 
    query = genIDQuery, 
    mapping = {
        "src" : df_join, 
        "mid" : dynf_tgt
    }
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713692901617_df = df_genID.toDF()
AWSGlueDataCatalog_node1713692901617 = glueContext.write_data_frame.from_catalog(frame=AWSGlueDataCatalog_node1713692901617_df, database="thesis", table_name="d_product", additional_options={})

job.commit()