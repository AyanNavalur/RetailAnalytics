from pyspark.sql import SparkSession


def get_spark_session(app_name):
    return (SparkSession
            .builder
            .appName(app_name)
            .master("local")
            .getOrCreate())
