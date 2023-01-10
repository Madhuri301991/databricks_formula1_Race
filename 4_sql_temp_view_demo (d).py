# Databricks notebook source
# MAGIC %md 
# MAGIC # Access dataframe using SQL 
# MAGIC 1. Create temporary views on dataframe 
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %md 
# MAGIC #Access the view from SQL cell

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT *
# MAGIC from v_race_results 
# MAGIC where race_year=2020

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*)
# MAGIC from v_race_results 
# MAGIC where race_year=2020

# COMMAND ----------

# MAGIC %md
# MAGIC # Access the view from Python cell

# COMMAND ----------

race_results_2019_df=spark.sql("select * from v_race_results where race_year=2019")

# COMMAND ----------

p_race_year=2020

# COMMAND ----------

race_results_2019_df=spark.sql(f'select * from v_race_results where race_year={p_race_year}')

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global temporary views 
# MAGIC 1. Create global temporary views on dataframe 
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")
#global views are stored in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results").show()

# COMMAND ----------

#access gv_race_results from another notebook 5_sql_temp_view_demo

