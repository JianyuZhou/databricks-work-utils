-- Databricks notebook source
CREATE TABLE main.default.students (name VARCHAR(64), address VARCHAR(64), student_id INT);
INSERT INTO main.default.students VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);

-- COMMAND ----------

SELECT * FROM main.default.students

-- COMMAND ----------


