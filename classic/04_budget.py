# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM movie_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM movie_silver

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

dbutils.fs.rm(rawPathMovie, recurse=True)

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

bronzeDFMovie = read_batch_bronze(spark)
transformedBronzeDFMovie = transform_bronze_movie(bronzeDFMovie)

(silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes_movie_budget(
    transformedBronzeDFMovie
)

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanDF, exclude_columns=["value"]
)
bronzeToSilverWriter.save(silverPathMovie)

update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")

# COMMAND ----------

display(bronzeDFMovie)
