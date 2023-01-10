# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step1 : Read the CSV file using the spark dataframe reader API 

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                   StructField('year',IntegerType(),True),
                                   StructField('round',IntegerType(),True),
                                   StructField('circuitId',IntegerType(),True),
                                   StructField('name',StringType(),True),
                                   StructField('date',DateType(),True),
                                   StructField('time',StringType(),True),
                                   StructField('url',StringType(),True)
                                  ]
                          )

# COMMAND ----------

races_df=spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Add ingestion date and race_timestamp to the dataframe 

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat, col,lit
#lit stands for literal

# COMMAND ----------

races_df=add_ingestion_date(races_df)

# COMMAND ----------

races_with_timestamp_df=races_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
                                .withColumn('data_source',lit(v_data_source))\
                                .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Select only the columns and rename as required

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col("raceId").alias('race_id'),col("year").alias('race_year'),col("round"),col('circuitId').alias('circuit_id'),col("name"),col("ingestion_date"),col("race_timestamp"),col("data_source"),col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the output to processed container in parquet format

# COMMAND ----------

#races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")
#partition the records into separate folders according to race_year

#races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")


races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1projectdeltalake/processed/races

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1projectdeltalake/processed/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")
