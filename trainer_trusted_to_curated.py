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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1672757823177 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1672757823177",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1672757884863 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1672757884863",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1672758395133 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1672758395133",
)

# Script generated for node Customer Join
CustomerJoin_node1672758502985 = Join.apply(
    frame1=AccelerometerTrusted_node1672757884863,
    frame2=CustomerTrusted_node1672758395133,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerJoin_node1672758502985",
)

# Script generated for node Join All
JoinAll_node2 = Join.apply(
    frame1=StepTrainerTrusted_node1672757823177,
    frame2=CustomerJoin_node1672758502985,
    keys1=["serialnumber", "sensorreadingtime"],
    keys2=["serialnumber", "timestamp"],
    transformation_ctx="JoinAll_node2",
)

# Script generated for node Drop Fields
DropFields_node1672758729759 = DropFields.apply(
    frame=JoinAll_node2,
    paths=[
        "customername",
        "email",
        "birthday",
        "registrationdate",
        "phone",
        "sharewithfriendsasofdate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "lastupdatedate",
    ],
    transformation_ctx="DropFields_node1672758729759",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.getSink(
    path="s3://stedi-lake-house-sb/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node3",
)
MachineLearningCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node3.setFormat("json")
MachineLearningCurated_node3.writeFrame(DropFields_node1672758729759)
job.commit()
