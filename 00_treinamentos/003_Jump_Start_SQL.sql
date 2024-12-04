-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC # 🧱 Databricks Jump Start for SQL 
-- MAGIC
-- MAGIC In this notebook we provide SQL code snippets examples of reading and writing data using Databricks.
-- MAGIC
-- MAGIC 📓 Do you like documentation? Who doesn't?! Here is [Pyspark SQL syntax documentation.](https://spark.apache.org/docs/2.3.0/api/sql/)
-- MAGIC
-- MAGIC #### Housekeeping items
-- MAGIC -         the database connections will need to be updated for your environment
-- MAGIC -         You must be connected to VPN to connect to your data sources
-- MAGIC -         Import the notebook file into your databricks workspace
-- MAGIC -         Make sure that your notebook is attached to one of your team's clusters
-- MAGIC -         You will have to have a cluster running to execute this code
-- MAGIC
-- MAGIC
-- MAGIC last updated: 2023-05-01
-- MAGIC
-- MAGIC *tldr: Make common tasks in databricks easy*

-- COMMAND ----------

-- MAGIC %md ## 📁 How to find and view data
-- MAGIC
-- MAGIC The following commands show how to view the data in your workspace. We are going to use magic commands to switch languages and run auxiliary commands.

-- COMMAND ----------

-- MAGIC %md ####List folders and files using the magic command %fs

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/.mnt/

-- COMMAND ----------

-- MAGIC %md ### List files in a folder

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/airlines

-- COMMAND ----------

-- MAGIC %md ### Look at the contents of a file

-- COMMAND ----------

-- MAGIC %fs head /databricks-datasets/airlines/part-00000/

-- COMMAND ----------

-- MAGIC %md ## 🧰 Create a database, table or view
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create a Database
-- MAGIC Here we will create database 'Airline_db' id a database with the same name doesn't exist. If it exists, it will not throw an error.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Airline_db;

-- COMMAND ----------

-- MAGIC %md ### Create a Table
-- MAGIC This is a SQL notebook, but this is the most common pattern of loading data (CSV, JSON, Parquet, Delta, Image, ect..) into a dataframe using python, which will then be used to create a table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Path to original data
-- MAGIC dir = "dbfs:/databricks-datasets/airlines/part-00000"
-- MAGIC
-- MAGIC # Read in the csv data into a dataframe using Spark
-- MAGIC bronzeDF = spark.read.format('csv')\
-- MAGIC   .option('delimiter', ',')\
-- MAGIC   .option('header', True)\
-- MAGIC   .load(dir)
-- MAGIC
-- MAGIC # This writes the dataframe to a table, which makes it SQL friendly
-- MAGIC bronzeDF.write \
-- MAGIC   .format("delta") \
-- MAGIC   .mode("overwrite")\
-- MAGIC   .saveAsTable("Airline_db.bronze_data") 
-- MAGIC
-- MAGIC display(bronzeDF) #Displays force execution, not generally recommended for prod

-- COMMAND ----------

-- MAGIC %md ### Create a table from [s3](https://docs.databricks.com/data/tables.html#create-a-table)
-- MAGIC This is commented out because it will only run within the VPN. Please adjust as needed to create a table in your workspace.

-- COMMAND ----------

CREATE TABLE <example-table>(id STRING, value STRING) USING org.apache.spark.sql.parquet OPTIONS (PATH "<your-storage-path>")

-- COMMAND ----------

-- MAGIC %md ### Create a Temp View from Parquet files
-- MAGIC Temporary views are session-scoped and is dropped when session ends because it skips persisting the definition in the underlying metastore, if any. Temporary views, like tables, make the data easy to query.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW airline_view
USING CSV
OPTIONS (path "dbfs:/databricks-datasets/airlines/part-00000", header "true");--you may have to adjust the filename and path for your workspace.

SELECT * FROM airline_view;

-- COMMAND ----------

-- MAGIC %md ### Create a Temp view from a csv
-- MAGIC
-- MAGIC Here is how to read in a csv file using SQL. If you want to import a txt, tsv or excel file, use a python library.

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered

CREATE OR REPLACE TEMPORARY VIEW airport_delays
USING CSV
OPTIONS (path "/FileStore/airlines.csv", header "true", mode "FAILFAST"); --you may have to adjust the filename and path for your workspace.

SELECT * FROM airport_delays;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### List Databases
-- MAGIC Your data may already be available in a database, here is how to view all the databases in your workspace.

-- COMMAND ----------

-- DBTITLE 1,List all databases
SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %md There may be a lot of databases, you can refine the list by adding query parameters.

-- COMMAND ----------

-- DBTITLE 0,List databases based on a parameter
SHOW DATABASES like 'Airline*';

-- COMMAND ----------

-- MAGIC %md ### List Tables within a Database
-- MAGIC
-- MAGIC View tables in a specific database

-- COMMAND ----------

-- DBTITLE 0,List tables in a database
SHOW TABLES FROM Airline_db

-- COMMAND ----------

-- MAGIC %md ### Query from CSV

-- COMMAND ----------

SELECT * FROM csv.`/FileStore/airlines.csv`

-- COMMAND ----------

-- MAGIC %md ### Query a delta table

-- COMMAND ----------

SELECT 
  Origin
  ,Dest
  ,Year
  ,Month
  ,DayofMonth
  ,DepTime
  ,ArrTime
FROM 
  Airline_db.bronze_data

-- COMMAND ----------

-- MAGIC %md ### Create a widget
-- MAGIC You can create [widget](https://docs.databricks.com/notebooks/widgets.html) with SQL
-- MAGIC

-- COMMAND ----------

-- MAGIC %md SQL and python work nicely together, here is how you can use a parameterized query in a python cell.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text('Dest', 'DFW') #create the parameter textbox
-- MAGIC dest = dbutils.widgets.get('Dest') #grab the textbox value
-- MAGIC dbutils.widgets.text('Origin', 'SFO') 
-- MAGIC origin = dbutils.widgets.get('Origin')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC query = spark.sql("""
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   Airline_db.bronze_data
-- MAGIC WHERE
-- MAGIC   (origin = '{}' OR '{}' = '')
-- MAGIC   AND (dest = '{}' OR '{}' = '')
-- MAGIC """.format(origin, origin, dest, dest))
-- MAGIC
-- MAGIC display(query)

-- COMMAND ----------

-- MAGIC %md ### Remove a Widget

-- COMMAND ----------

REMOVE WIDGET Dest;
REMOVE WIDGET Origin;


-- COMMAND ----------

-- MAGIC %md ## *️⃣ Other Tasks

-- COMMAND ----------

-- MAGIC %md ###Window Functions
-- MAGIC
-- MAGIC Use [window functions](https://docs.databricks.com/sql/language-manual/sql-ref-window-functions.html) to apply operations on a group of rows, referred to as a window, and calculate a return value for each row based on the group of rows. Window functions are useful for processing tasks such as calculating a moving average, computing a cumulative statistic, or accessing the value of rows given the relative position of the current row.
-- MAGIC

-- COMMAND ----------

select UniqueCarrier, DepDelay,
       max(avg_delay) over() as max_delayed
from (
  select UniqueCarrier,
        DepDelay,
         avg(DepDelay) as avg_delay
  from Airline_db.bronze_data
  group by UniqueCarrier, DepDelay) as s

  

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### Create a function
-- MAGIC Creates a SQL scalar or table [function](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html) that takes a set of arguments and returns a scalar value or a set of rows. If you are running this notebook for the first time, comment out the 'DROP FUNCTION' line.

-- COMMAND ----------

DROP FUNCTION square;
CREATE FUNCTION square(x DOUBLE) RETURNS DOUBLE RETURN x * x;
SELECT square(42)

-- COMMAND ----------

DROP FUNCTION roll_dice;
CREATE FUNCTION roll_dice(num_dice  INT DEFAULT 1 COMMENT 'number of dice to roll (Default: 1)',
                            num_sides INT DEFAULT 6 COMMENT 'number of sides per die (Default: 6)')
    RETURNS INT
    NOT DETERMINISTIC
    CONTAINS SQL
    COMMENT 'Roll a number of n-sided dice'
    RETURN aggregate(sequence(1, roll_dice.num_dice, 1),
                     0,
                     (acc, x) -> (rand() * roll_dice.num_sides)::int,
                     acc -> acc + roll_dice.num_dice);

-- Roll a single 6-sided die still works
SELECT roll_dice();

-- COMMAND ----------

-- MAGIC %md ### Create a UDF

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # CREATE UDF to convert TIME to an INTEGER .. Ex: 1348 --> 828
-- MAGIC def getMinuteOfDay(time):
-- MAGIC   try:
-- MAGIC     temp = str(('0000' + str(time))[-4:])
-- MAGIC     print(temp)
-- MAGIC     print(temp.isdigit())
-- MAGIC     if temp.isdigit():
-- MAGIC       return int(temp[:2]) * 60 + int(temp[-2:])
-- MAGIC     return
-- MAGIC   except:
-- MAGIC     return
-- MAGIC   
-- MAGIC spark.udf.register("getMinuteOfDay", getMinuteOfDay)

-- COMMAND ----------

-- MAGIC %md ### Test the UDF

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getMinuteOfDay(741)

-- COMMAND ----------

-- MAGIC %md ### Use a python function in SQL

-- COMMAND ----------

SELECT 
  Origin
  ,Dest
  ,Year
  ,Month
  ,DayofMonth
  ,DepTime
  ,getMinuteOfDay(DepTime) DepTimeInMinutes
  ,ArrTime
  ,getMinuteOfDay(ArrTime) AS ArrTimeInMinutes 
FROM 
  Airline_db.bronze_data

-- COMMAND ----------

-- MAGIC %md ### View the history of a table

-- COMMAND ----------

DESCRIBE HISTORY Airline_db.bronze_data;

-- COMMAND ----------

-- MAGIC %md ### Time Travel to view older version of table

-- COMMAND ----------

SELECT * FROM Airline_db.bronze_data VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md ### Describe detail of table

-- COMMAND ----------

DESCRIBE DETAIL Airline_db.bronze_data

-- COMMAND ----------

-- MAGIC %md ### Describe format

-- COMMAND ----------

DESCRIBE FORMATTED Airline_db.bronze_data

-- COMMAND ----------

-- MAGIC %md ### Optimize and Zorder a table

-- COMMAND ----------

OPTIMIZE Airline_db.bronze_data ZORDER BY Origin, UniqueCarrier

-- COMMAND ----------

-- MAGIC %md ### Add Metadata to a table
-- MAGIC Comments can be added by column or at the table levelto can help other users better understand data definitions or any caveats about using a particular field.
-- MAGIC
-- MAGIC Comments will also appear in the UI to further help other users understand the table and columms:

-- COMMAND ----------

ALTER TABLE Airline_db.bronze_data ALTER COLUMN TailNum COMMENT 'This field is sparsly populated';
ALTER TABLE Airline_db.bronze_data SET TBLPROPERTIES ('comment' = 'This is a table comment.Airline Data Raw Table with delay information');

-- COMMAND ----------

SELECT avg(DepDelay) as avg_DepDelay, Origin from Airline_db.bronze_data group by Origin order by avg_DepDelay desc;

-- COMMAND ----------

-- MAGIC %md ### CSV
-- MAGIC
-- MAGIC Here is how to read in a csv file using SQL. If you want to import a txt, tsv or excel file, use a python library.

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
 
CREATE OR REPLACE TEMPORARY VIEW airport_delays
USING CSV
OPTIONS (path "/FileStore/airlines.csv", header "true", mode "FAILFAST"); 
--you may have to adjust the filename and path for your workspace.
 
SELECT * FROM airport_delays;
