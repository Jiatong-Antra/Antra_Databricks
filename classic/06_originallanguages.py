# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

bronzeDFMovie = spark.read.table("movie_bronze")

# COMMAND ----------

display(bronzeDFMovie)

# COMMAND ----------

silverDFmovie = transform_bronze_movie(bronzeDFMovie)
display(silverDFmovie)

# COMMAND ----------

from pyspark.sql.functions import *
originallanguageDFmovie = silverDFmovie.select("OriginalLanguage")

display(originallanguageDFmovie)

# COMMAND ----------

print((originallanguageDFmovie.count(), len(originallanguageDFmovie.columns)))

originallanguageDFmovie = originallanguageDFmovie.dropDuplicates()

print((originallanguageDFmovie.count(), len(originallanguageDFmovie.columns)))

display(originallanguageDFmovie)


# COMMAND ----------

(originallanguageDFmovie.select('OriginalLanguage')  
  .write.format("delta")
  .mode('overwrite')
  .save(originallanguagessilverPathMovie)
)


spark.sql(
    """
DROP TABLE IF EXISTS originallanguage_silver
"""
)

spark.sql(
    f"""
CREATE TABLE originallanguage_silver
USING DELTA
LOCATION "{originallanguagessilverPathMovie}"
"""
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM originallanguage_silver
