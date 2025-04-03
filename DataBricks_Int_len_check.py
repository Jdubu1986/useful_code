import time  # For timing the script execution
import IPython.display as ipd  # For clearing output in Jupyter notebooks
from pyspark.sql.functions import when, col  # For conditional logic in Spark DataFrames

# Aliases for location-specific information (replace with your actual values)
DATABASE_NAME = "my_database"  # Name of your database
SCHEMA_NAME = "my_schema"      # Name of your schema

# Query to get table and column information for integer columns
info_schema_query = f"""
SELECT table_catalog, table_schema, table_name, column_name, data_type
FROM system.information_schema.columns
WHERE data_type IN ('INT')  # Select only integer columns
AND table_catalog in ('{DATABASE_NAME}')  # Filter by the specified database
"""

# Execute the query and get the result as a DataFrame
info_df = spark.sql(info_schema_query)

# Collect the schema, table, and column information into a list of Row objects
columns_info = info_df.collect()

# Check if any integer columns were found
if not columns_info:
    print("No integer columns found.")
else:
    # Function to get the maximum value for a given table and column
    def get_max_value(table_catalog, table_schema, table_name, column_name, data_type):
        """Retrieves the maximum value of a specific column in a table."""
        query = f"SELECT MAX(`{column_name}`) as max_value FROM `{table_catalog}`.`{table_schema}`.`{table_name}`"
        result_df = spark.sql(query)
        max_value = result_df.collect()[0]['max_value']  # Extract the max value from the DataFrame
        return max_value

    # Iterate over the columns and get the max value for each integer column
    max_values = []  # List to store the results
    total_columns = len(columns_info)  # Total number of columns to process
    start_time = time.time()  # Start time of the processing

    for i, row in enumerate(columns_info, start=1):
        catalog = row['table_catalog']
        schema = row['table_schema']
        table = row['table_name']
        column = row['column_name']
        data_type = row['data_type']

        try:
            max_value = get_max_value(catalog, schema, table, column, data_type) #get the max value
            max_values.append((catalog, schema, table, column, data_type, max_value)) #append the results.
        except Exception as e:
            print(f"Error processing {catalog}.{schema}.{table}.{column}: {e}") #print the error.

        # Calculate progress and estimated remaining time
        percentage = (i / total_columns) * 100
        elapsed_time = time.time() - start_time
        avg_time_per_iteration = elapsed_time / i
        remaining_time = avg_time_per_iteration * (total_columns - i)

        # Clear previous output and display progress
        ipd.clear_output(wait=True)
        print(f"Iteration {i}/{total_columns} ({percentage:.2f}%) - Estimated remaining time: {remaining_time / 60:.2f} minutes")

    # Check if any max values were found
    if not max_values:
        print("No max values found.")
    else:
        # Convert the results to a DataFrame for better visualization
        max_values_df = spark.createDataFrame(max_values, ["catalog", "schema", "table", "column", "data_type", "max_value"])

        # Add risk_level column based on max_value thresholds
        max_values_df = max_values_df.withColumn(
            "risk_level",
            when(col("max_value") > 2000000000, "High Risk")  # High risk if max value > 2 billion
            .when(col("max_value") < 0, "Negative Value") # negative values
            .when(col("max_value") > 1500000000, "Medium Risk") # medium risk.
            .otherwise("Low Risk") #low risk.
        )
        # Display the resulting DataFrame
        display(max_values_df)