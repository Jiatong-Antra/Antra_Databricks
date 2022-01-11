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
genresDFmovie = silverDFmovie.select(explode("genres").alias("genres"))

display(genresDFmovie)

# COMMAND ----------

genresDFmovie = genresDFmovie.select("genres.id", "genres.name")

display(genresDFmovie)


# COMMAND ----------




print((genresDFmovie.count(), len(genresDFmovie.columns)))

genresDFmovie = genresDFmovie.dropDuplicates((['id']))

print((genresDFmovie.count(), len(genresDFmovie.columns)))

display(genresDFmovie)



# COMMAND ----------

(genresDFmovie.select('id',
                       'name')  
  .write.format("delta")
  .mode('overwrite')
  .save(genressilverPathMovie)
)


spark.sql(
    """
DROP TABLE IF EXISTS genres_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genres_silver
USING DELTA
LOCATION "{genressilverPathMovie}"
"""
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM genres_silver
