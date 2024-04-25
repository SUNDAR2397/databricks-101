-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create managed tables in the silver schema
-- MAGIC 1. drivers
-- MAGIC 2. results

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.silver.drivers;

CREATE TABLE IF NOT EXISTS formula1_dev.silver.drivers
AS 
SELECT driverId  AS driver_id,
       driverRef AS driver_ref,
       number,
       code,
       concat(name.forename, ' ', name.surname) AS name,     
       dob,
       nationality,
       current_timestamp() AS ingestion_date  
  FROM formula1_dev.bronze.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.silver.results;

CREATE TABLE IF NOT EXISTS formula1_dev.silver.results
AS 
SELECT  resultId AS result_id,
        raceId AS race_id,
        driverId AS driver_id,
        constructorId AS constructor_id,
        number,
        grid,
        position,
        positionText AS position_text,
        positionOrder AS position_order,
        points,
        laps,
        time,
        milliseconds,
        fastestLap fastest_lap,
        rank,
        fastestLapTime AS fastest_lap_time,
        fastestLapSpeed AS fastest_lap_speed,
        statusId AS status_id,
        current_timestamp() AS ingestion_date  
  FROM formula1_dev.bronze.results;

-------------------------------------------------------------------------------------------------------------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Define your schema according to the structure of your Bronze table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("metadata", StringType(), True)  # Example of a complex type, could be further nested
])

--------------------------------------------------------------------------------------------------------------
# Load the data from the Bronze table
df_bronze = spark.table("bronze_table_name")

# Perform transformations if necessary
df_silver = df_bronze.select("id", "name", "date_of_birth", "metadata")

# Write to the Silver table, you can also enforce the schema if needed
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_table_name")

----------------------------------------------------------------------------------------------------------------------
# Assuming the Silver table is already loaded as a DataFrame
df_silver = spark.table("silver_table_name")

# Define a new DataFrame with custom columns
df_view = df_silver.withColumn("age", (2023 - year(df_silver["date_of_birth"]))).withColumn("name_uppercase", upper(df_silver["name"]))

# Create a temporary view
df_view.createOrReplaceTempView("silver_view")

# Now the view can be used in Spark SQL
spark.sql("SELECT id, name_uppercase, age FROM silver_view").show()

# Assuming df_bronze already adheres to the correct schema
df_silver = df_bronze.select(
    col("id").cast(IntegerType()),
    col("name").cast(StringType()),
    col("date_of_birth").cast(DateType()),
    col("metadata").cast(StringType())
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_table_name")



-- COMMAND ----------

