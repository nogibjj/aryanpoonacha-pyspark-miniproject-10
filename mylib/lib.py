"""
Extract dataset into sql db
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, udf
from pyspark.sql.types import FloatType

def init_spark(app_name="PlanetAnalysis"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, file_path='tables/planets.csv'):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def rastr_to_decimal(rastr):
    '''
    Convert Right Ascension from sexagesimal string format to decimal degrees.
    The input is a string in the format "HH:MM:SS.ss".
    '''
    try:
        h, m, s = map(float, rastr.split(':'))
        ra = 15 * (h + m/60 + s/3600)
        return ra
    except ValueError:
        print(f"Error: Invalid RA string {rastr}")
        return None

def decstr_to_decimal(decstr):
    '''
    Convert Declination from sexagesimal string format to decimal degrees.
    The input is a string in the format "[-]DD:MM:SS.ss".
    '''
    try:
        d, m, s = map(float, decstr.split(':'))
        sign = -1 if decstr.strip()[0] == '-' else 1
        dec = sign * (abs(d) + m/60 + s/3600)
        return dec
    except ValueError:
        print(f"Error: Invalid Dec string {decstr}")
        return None

def clean_transform_data(df):
    '''
    Cleans and transforms the data
    '''
    # Register the functions as UDFs
    rastr_to_decimal_udf = udf(rastr_to_decimal, FloatType())
    decstr_to_decimal_udf = udf(decstr_to_decimal, FloatType())

    # Remove rows with null values in specific columns
    df = df.na.drop(subset=["pl_name", "hostname", "disc_year"])

    # Replace specific values
    df = df.withColumn('pl_orbper', when(col('pl_orbper') == 'Unknown', None).otherwise(col('pl_orbper')))

    # Convert data types
    df = df.withColumn('pl_orbper', col('pl_orbper').cast('float'))
    df = df.withColumn('disc_year', col('disc_year').cast('int'))

    # Convert RA and Dec from sexagesimal to decimal
    df = df.withColumn('ra', rastr_to_decimal_udf('rastr'))
    df = df.withColumn('dec', decstr_to_decimal_udf('decstr'))

    # Add new columns
    df = df.withColumn('pl_orbper_squared', expr('pl_orbper * pl_orbper'))

    return df
