# Databricks notebook source
# MAGIC %md 
# MAGIC ## Spark Join Transformation 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
            .filter("circuit_id<70")\
            .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019")\
         .withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"inner")\
                .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df.select("circuit_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outer Join

# COMMAND ----------

#left outer join
races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"left")\
                .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

## right outer join
races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"right")\
                .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

## full outer join 
races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"full")\
                .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Semi-join 

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"semi")\
                .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)
#semi doesnt allow to take column from right table
#semi same as inner join but values are avilable only from left table not right table 

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"semi")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Anti Join 

# COMMAND ----------

# anti join is the oppositve of semi join 
# gives everything from left data frame not found on right data frame 
races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"anti")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

circuits_race_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,"anti")

# COMMAND ----------

display(circuits_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cross Join

# COMMAND ----------

#cross join gives cartesian product
circuits_race_df=races_df.crossJoin(circuits_df)

# COMMAND ----------

display(circuits_race_df)

# COMMAND ----------

circuits_race_df.count()

# COMMAND ----------

int(circuits_df.count())*int(races_df.count())
