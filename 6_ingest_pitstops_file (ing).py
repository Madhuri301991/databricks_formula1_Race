# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1 : Read the JSON file using the spark dataframe reader API

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

pitstops_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                  StructField('driverId',IntegerType(),True),
                                  StructField('stop',StringType(),True),
                                  StructField('lap',IntegerType(),True),
                                  StructField('time',StringType(),True),
                                  StructField('duration',StringType(),True),
                                  StructField('milliseconds',IntegerType(),True)
])

# COMMAND ----------

pitstops_df=spark.read.schema(pitstops_schema).option("multiLine",True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")
#json is a multiline file so put multiLine=True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Rename columns and add new column
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pitstops_df=add_ingestion_date(pitstops_df)

# COMMAND ----------

pitstops_final_df=pitstops_df.withColumnRenamed("raceId","race_id")\
                                  .withColumnRenamed("driverId","driver_id")\
                                   .withColumn('data_source',lit(v_data_source))

# COMMAND ----------

pitstops_dedupded_df=pitstops_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write output to processed container in delta

# COMMAND ----------

merge_condition="tgt.race_id=src.race_id AND tgt.driver_id=src.driver_id AND tgt.stop=src.stop"
merge_delta_data(pitstops_dedupded_df,'f1_processed','pitstops',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

#######pitstops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pitstops")


#pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstops")

#overwrite_partition(pitstops_final_df,'f1_processed','pitstops','race_id')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM f1_processed.pitstops;
# MAGIC --Drop table f1_processed.pitstops;
# MAGIC 
# MAGIC SELECT race_id,COUNT(1)
# MAGIC FROM f1_processed.pitstops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
