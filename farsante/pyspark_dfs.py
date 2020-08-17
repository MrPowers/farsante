from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def people_df():
    return spark.read.option('header', True).csv('./data/people.csv')


def iris_df():
    return spark.read.option('header', True).csv('./data/iris.csv')
