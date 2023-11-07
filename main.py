import mylib.lib as lib

def main():
    # Initialize Spark
    spark = lib.init_spark()

    # Load data
    df = lib.load_data(spark)

    # Clean and transform data
    df_transformed = lib.clean_transform_data(df)
    print(df_transformed)

    # Execute a SQL query
    query = "SELECT pl_name, hostname, pl_orbper FROM my_table ORDER BY pl_orbper DESC LIMIT 10"
    lib.execute_sql_query(spark, df_transformed, query)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
