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

# Script generated for node Customer Curated
CustomerCurated_node1672757036440 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1672757036440",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-sb/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Join
CustomerJoin_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1672757036440,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="CustomerJoin_node2",
)

# Script generated for node Drop Fields
DropFields_node1672757410134 = DropFields.apply(
    frame=CustomerJoin_node2,
    paths=[
        "customername",
        "email",
        "birthday",
        "registrationdate",
        "serialnumber",
        "phone",
        "sharewithfriendsasofdate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "lastupdatedate",
    ],
    transformation_ctx="DropFields_node1672757410134",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.getSink(
    path="s3://stedi-lake-house-sb/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node3",
)
StepTrainerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node3.setFormat("json")
StepTrainerTrusted_node3.writeFrame(DropFields_node1672757410134)
job.commit()
