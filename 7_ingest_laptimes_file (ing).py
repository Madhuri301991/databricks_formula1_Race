# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest lap_times.json folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1 : Read the csv file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source'," ")                #assign the value 
v_data_source=dbutils.widgets.get('p_data_source')       #get the value

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28") 
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

laptimes_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                  StructField('driverId',IntegerType(),True),
                                  StructField('lap',IntegerType(),True),
                                  StructField('position',IntegerType(),True),
                                  StructField('time',StringType(),True),
                                  StructField('milliseconds',IntegerType(),True)
])

# COMMAND ----------

laptimes_df=spark.read.schema(laptimes_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Rename columns and add new column
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

laptimes_df=add_ingestion_date(laptimes_df)

# COMMAND ----------

laptimes_final_df=laptimes_df.withColumnRenamed("raceId","race_id")\
                                  .withColumnRenamed("driverId","driver_id")\
                                  .withColumn('data_source',lit(v_data_source))

# COMMAND ----------

laptimes_dedupded_df=laptimes_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write output to processed container in parquet file

# COMMAND ----------

merge_condition="tgt.race_id=src.race_id AND tgt.driver_id=src.driver_id AND tgt.lap=src.lap"
merge_delta_data(laptimes_dedupded_df,'f1_processed','laptimes',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

#####laptimes_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/laptimes")


#laptimes_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptimes")

#overwrite_partition(laptimes_final_df,'f1_processed','laptimes','race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM f1_processed.laptimes;
# MAGIC --DROP table f1_processed.laptimes;
# MAGIC 
# MAGIC SELECT race_id,COUNT(1)
# MAGIC FROM f1_processed.laptimes
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
