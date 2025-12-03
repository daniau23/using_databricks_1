# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `emisssions`.`default`.`emissions_data_2023`;

# COMMAND ----------

import pyspark
pyspark.sql.functions

# COMMAND ----------

# MAGIC %md
# MAGIC **Same SQL Queries works here**

# COMMAND ----------

# MAGIC %md
# MAGIC These are the comments for why the Location_data.sql code was formatted
# MAGIC - -- Select latitude, longitude, and greenhouse gas emissions columns
# MAGIC - -- Use backticks for column names with spaces
# MAGIC - -- Use the fully qualified table name to avoid "table not found" errors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   latitude, 
# MAGIC   longitude, 
# MAGIC   `GHG emissions mtons CO2e` AS emissons
# MAGIC FROM emissions_data_2023

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# MAGIC %md
# MAGIC You can use pandas in Databricks notebooks, but it is recommended to use Spark DataFrames or the Pandas API on Spark for large datasets, as pandas only works efficiently with small data that fits in memory on a single machine
# MAGIC 1
# MAGIC 2
# MAGIC . For distributed processing and scalability, use Spark DataFrames or pyspark.pandas (Pandas API on Spark)

# COMMAND ----------

# Read the emissions data into a Spark DataFrame
df = spark.table(
    "emisssions.default.emissions_data_2023"
)

# Group by county_name and sum the population
result = df.groupBy(
    "county_name"
).sum(
    "population"
)

display(result)

# COMMAND ----------

import pyspark.sql.functions as F

# Read the emissions data and select relevant columns
df = spark.table(
    "emisssions.default.emissions_data_2023"
).select(
    "latitude",  # Facility latitude
    "longitude", # Facility longitude
    F.col("GHG emissions mtons CO2e").alias("emissons") # Greenhouse gas emissions (metric tons CO2e)
)

# Display the selected data
display(df)

# COMMAND ----------

import pyspark.sql.functions as F

# Read the emissions data into a Spark DataFrame
df = spark.table(
    "emisssions.default.emissions_data_2023"
)

# Group by state abbreviation and sum the GHG emissions (after removing commas and casting to double)
result = df.groupBy("state_abbr").agg(
    F.sum(
        F.regexp_replace(
            F.col("GHG emissions mtons CO2e"),
            ",",
            ""
        ).cast("double")
    ).alias("Total_Emissions")
).orderBy(
    F.col("Total_Emissions").desc()
)

# Display the result sorted by total emissions in descending order
display(result)
