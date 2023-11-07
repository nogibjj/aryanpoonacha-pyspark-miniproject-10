"""
Extract dataset into sql db
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr

def init_spark(app_name="PlanetAnalysis"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def load_data(spark, file_path='tables/planets.csv'):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def clean_transform_data(df):
    '''
    Cleans and transforms the data
    '''
    # Remove rows with null values in specific columns
    df = df.na.drop(subset=["pl_name", "hostname", "disc_year"])

    # Replace specific values
    df = df.withColumn('pl_orbper', when(col('pl_orbper') == 'Unknown', None).otherwise(col('pl_orbper')))

    # Convert data types
    df = df.withColumn('pl_orbper', col('pl_orbper').cast('float'))
    df = df.withColumn('disc_year', col('disc_year').cast('int'))

    # Convert RA and Dec from sexagesimal to decimal
    df = df.withColumn('ra', expr('rastr_to_decimal(rastr)'))
    df = df.withColumn('dec', expr('decstr_to_decimal(decstr)'))

    # Add new columns
    df = df.withColumn('pl_orbper_squared', expr('pl_orbper * pl_orbper'))

    return df
