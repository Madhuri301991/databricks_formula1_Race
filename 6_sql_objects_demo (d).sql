-- Databricks notebook source
-- MAGIC %md 
-- MAGIC 1. Spark Sql documentation 
-- MAGIC 2. Create database demo 
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. Describe command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE if not exists demo;

-- COMMAND ----------

Show databases;

-- COMMAND ----------

Describe database demo;

-- COMMAND ----------

Describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

Show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table 
-- MAGIC 4. Describe table 

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")
-- MAGIC #table gets created in Database demo

-- COMMAND ----------

desc Extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year=2020;

-- COMMAND ----------

create table demo.race_results_sql
as
select * from demo.race_results_python where race_year=2020;

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using Sql
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

Desc extended demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
(
race_name STRING,
race_year INT,
race_date TIMESTAMP,
circuit_location STRING,
driver_number INT,
driver_name STRING,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
using parquet
LOCATION "/mnt/formula1projectdeltalake/presentation/race_results_ext_sql"

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year=2020;

-- COMMAND ----------

select count(*) from demo.race_results_ext_sql;

-- COMMAND ----------

drop table demo.race_results_ext_sql;

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # views on table 
-- MAGIC 1. Create temp view
-- MAGIC 2. Create global temp view
-- MAGIC 3. Create permanent view

-- COMMAND ----------

create or replace temp view v_race_results
as
select * from demo.race_results_python where race_year=2018;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view gv_race_results
as
select * from demo.race_results_python where race_year=2012;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

create or replace view demo.pv_race_results
as
select * from demo.race_results_python
where race_year=2000;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from demo.pv_race_results
