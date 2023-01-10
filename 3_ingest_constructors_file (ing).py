# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 : Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text('p_data_source'," ")                #assign the value 
v_data_source=dbutils.widgets.get('p_data_source')       #get the value

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING "

# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2 : Drop the unwanted column from the Dataframe

# COMMAND ----------

constructors_dropped_df=constructors_df.drop("url")

# COMMAND ----------

#second method to drop
constructors_dropped_df=constructors_df.drop(constructors_df["url"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#third method to drop
constructors_dropped_df=constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_final_df=constructors_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                             .withColumnRenamed("constructorRef","constructor_ref")\
                                              .withColumn('data_source',lit(v_data_source))\
                                              .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

constructors_final_df=add_ingestion_date(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write output to parquet file

# COMMAND ----------

#constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
#constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("success")
