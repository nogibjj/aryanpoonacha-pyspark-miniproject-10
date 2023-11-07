import mylib.lib as lib

def main():
    # Initialize Spark
    spark = lib.init_spark()

    # Load data
    df = lib.load_data(spark)

    # Clean and transform data
    df_transformed = lib.clean_transform_data(df)
    print(df_transformed)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
