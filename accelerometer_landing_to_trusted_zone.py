import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1672748834092 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-sb/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1672748834092",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1672749051147 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1672749051147",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=CustomerTrustedZone_node1672748834092,
    frame2=AccelerometerLanding_node1672749051147,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1672749327758 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1672749327758",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1672749327758,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-sb/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
