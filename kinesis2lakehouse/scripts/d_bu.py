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

# Script generated for node Get Last Query Time
GetLastQueryTime_node1721756802200 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://trinm-lakehouse/config/load_config.json"], "recurse": True}, transformation_ctx="GetLastQueryTime_node1721756802200")

# Script generated for node TGT DIM BU
TGTDIMBU_node1721756647233_df = glueContext.create_data_frame.from_catalog(database="thesis", table_name="d_bu")
TGTDIMBU_node1721756647233 = DynamicFrame.fromDF(TGTDIMBU_node1721756647233_df, glueContext, "TGTDIMBU_node1721756647233")

# Script generated for node RAW JSON
RAWJSON_node1721756555732 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://trinm-lakehouse/raw/sales/"], "recurse": True}, transformation_ctx="RAWJSON_node1721756555732")

# Script generated for node Rename for joining
Renameforjoining_node1721756915783 = ApplyMapping.apply(frame=TGTDIMBU_node1721756647233, mappings=[("id", "int", "id", "int"), ("business_unit", "string", "right_business_unit", "string"), ("province_lv1", "string", "right_province_lv1", "string"), ("province_lv2", "string", "right_province_lv2", "string")], transformation_ctx="Renameforjoining_node1721756915783")

# Script generated for node Delta Load
SqlQuery1 = '''
select distinct
    business_unit,
    province_lv1,
    province_lv2
from
    myDataSource
where
    event_timestamp >= (
        select
            sales.dim_bu.last_read
        from
            lstQ
    )
'''
DeltaLoad_node1721756985549 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":RAWJSON_node1721756555732, "lstQ":GetLastQueryTime_node1721756802200}, transformation_ctx = "DeltaLoad_node1721756985549")

# Script generated for node Get new data
DeltaLoad_node1721756985549DF = DeltaLoad_node1721756985549.toDF()
Renameforjoining_node1721756915783DF = Renameforjoining_node1721756915783.toDF()
Getnewdata_node1721757070046 = DynamicFrame.fromDF(DeltaLoad_node1721756985549DF.join(Renameforjoining_node1721756915783DF, (DeltaLoad_node1721756985549DF['business_unit'] == Renameforjoining_node1721756915783DF['right_business_unit']) & (DeltaLoad_node1721756985549DF['province_lv1'] == Renameforjoining_node1721756915783DF['right_province_lv1']) & (DeltaLoad_node1721756985549DF['province_lv2'] == Renameforjoining_node1721756915783DF['right_province_lv2']), "leftanti"), glueContext, "Getnewdata_node1721757070046")

# Script generated for node Generate ID
SqlQuery0 = '''
select
    ROW_NUMBER() OVER (
        ORDER BY
            business_unit
    ) + (
        SELECT
            ifnull (max(id), 0)
        FROM
            mid
    ) id,
    business_unit,
    province_lv1,
    province_lv2
from
    src
'''
GenerateID_node1721757171341 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"src":Getnewdata_node1721757070046, "mid":TGTDIMBU_node1721756647233}, transformation_ctx = "GenerateID_node1721757171341")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1721757232485_df = GenerateID_node1721757171341.toDF()
AWSGlueDataCatalog_node1721757232485 = glueContext.write_data_frame.from_catalog(frame=AWSGlueDataCatalog_node1721757232485_df, database="thesis", table_name="d_bu", additional_options={})

job.commit()