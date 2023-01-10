# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake(managed table)
# MAGIC 2. Write data to delta lake(external table)
# MAGIC 3. Read data from delta lake(Table)
# MAGIC 4. Read data from delta lake(File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1projectdeltalake/demo";

# COMMAND ----------

results_df=spark.read.option("inferSchema",True).json("/mnt/formula1projectdeltalake/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1projectdeltalake/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1projectdeltalake/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external;

# COMMAND ----------

results_external_df=spark.read.format("delta").load("/mnt/formula1projectdeltalake/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. UPDATE FROM DELTA TABLE 
# MAGIC 2. DELETE FROM DELTA TABLE

# COMMAND ----------

# MAGIC  %sql
# MAGIC  SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points=11-position 
# MAGIC WHERE position<=10;

# COMMAND ----------

# MAGIC  %sql
# MAGIC  SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

#update code for python
from delta.tables import DeltaTable
deltaTable=DeltaTable.forPath(spark,"/mnt/formula1projectdeltalake/demo/results_managed")
deltaTable.update("position<=10",{"points":"21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position>10;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

#delete code for python
from delta.tables import DeltaTable
deltaTable=DeltaTable.forPath(spark,"/mnt/formula1projectdeltalake/demo/results_managed")
deltaTable.delete("points=0")

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC upsert using merge 

# COMMAND ----------

drivers_day1_df=spark.read.option("inferSchema",True)\
.json("/mnt/formula1projectdeltalake/raw/2021-03-28/drivers.json")\
.filter("driverId<=10").select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df=spark.read.option("inferSchema",True)\
.json("/mnt/formula1projectdeltalake/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 AND 15")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df=spark.read.option("inferSchema",True)\
.json("/mnt/formula1projectdeltalake/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC Day1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId=upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob=upd.dob,
# MAGIC 	   tgt.forename=upd.forename,
# MAGIC 	   tgt.surname=upd.surname,
# MAGIC 	   tgt.updatedDate=current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC  THEN INSERT (driverId,dob,forename,surname,createdDate) VALUES (driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId=upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob=upd.dob,
# MAGIC 	   tgt.forename=upd.forename,
# MAGIC 	   tgt.surname=upd.surname,
# MAGIC 	   tgt.updatedDate=current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC  THEN INSERT (driverId,dob,forename,surname,createdDate) VALUES (driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable 
deltaTable=DeltaTable.forPath(spark,"/mnt/formula1projectdeltalake/demo/drivers_merge")
deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),"tgt.driverId=upd.driverId")\
    .whenMatchedUpdate(set={"dob":"upd.dob","forename":"upd.forename","surname":"upd.surname","updatedDate":"current_timestamp()"})\
    .whenNotMatchedInsert(values=
    {
    "driverId":"upd.driverId",
    "dob":"upd.dob",
    "forename":"upd.forename",
    "surname":"upd.surname",
    "createdDate":"current_timestamp()"
    }
   )\
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History and versioning 
# MAGIC 2. Time Travel 
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2023-01-04T21:32:11.000+0000'

# COMMAND ----------

df=spark.read.format("delta").option("timestampAsOf",'2023-01-04T21:32:11.000+0000').load("/mnt/formula1projectdeltalake/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.format("delta").option("VERSIONAsOf",'1').load("/mnt/formula1projectdeltalake/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge;
# MAGIC -- removes data older than 7 days 
# MAGIC -- before deleting, it retains data for 7 days 

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS;
# MAGIC -- immediately delets data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 3;
# MAGIC -- restores records before deletion of record driverid=1

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Restores deleted record 
# MAGIC --- inserts deleted record back into table
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC   ON (tgt.driverId=src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

for driver_id in range(3,20):
    spark.sql(f"INSERT INTO f1_demo.drivers_txn SELECT * FROM f1_demo.drivers_merge WHERE driverId={driver_id}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;
# MAGIC -- convert parquet table to delta 

# COMMAND ----------

df=spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

#creating parquet file 
df.write.format("parquet").save("/mnt/formula1projectdeltalake/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA PARQUET .`/mnt/formula1projectdeltalake/demo/drivers_convert_to_delta_new`
# MAGIC -- convert parquet file to delta

# COMMAND ----------


