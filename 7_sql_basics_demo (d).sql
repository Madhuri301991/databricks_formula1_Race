-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers LIMIT 10;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * FROM drivers WHERE nationality="British" and dob>='1990-01-01';

-- COMMAND ----------

SELECT name,dob as date_of_birth
FROM drivers WHERE nationality="British" and dob>='1990-01-01'
order by dob desc;

-- COMMAND ----------

select * from drivers order by nationality asc, dob desc;

-- COMMAND ----------

SELECT name,dob as date_of_birth,nationality
FROM drivers WHERE (nationality="British" and dob>='1990-01-01') or nationality='Indian'
order by dob desc;
