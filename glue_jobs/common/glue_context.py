import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


def initialize_glue_job():
    """
    Initializes AWS Glue Context, Spark Session and Job.
    """

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    return glue_context, spark, job