# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md  
# MAGIC # 🧱 Databricks Jump Start
# MAGIC
# MAGIC In this notebook we provide code snippets examples of reading and writing data using Databricks.
# MAGIC
# MAGIC #### Housekeeping items
# MAGIC -         the database connections will need to be updated for your environment
# MAGIC -         You must be connected to VPN to connect to your data sources
# MAGIC -         Import the notebook file into your databricks workspace
# MAGIC -         Make sure that your notebook is attached to one of your team's clusters
# MAGIC -         You will have to have a cluster running to execute this code
# MAGIC
# MAGIC
# MAGIC last updated: 2023-05-01
# MAGIC
# MAGIC *tldr: Make common tasks in databricks easy*

# COMMAND ----------

displayHTML("<h1><marquee>This notebook is for code reference, some cells will not run and that is okay </marquee></h1>")

# COMMAND ----------

# MAGIC %md ## 📁 How to find and view data
# MAGIC
# MAGIC The following commands show how to view the data in your workspace. We are going to use magic commands to switch languages and run auxiliary commands.
# MAGIC
# MAGIC **Language Magic Commands**
# MAGIC -  %python
# MAGIC -  %r
# MAGIC -  %scala
# MAGIC -  %sql
# MAGIC
# MAGIC **Auxiliary Magic Commands**
# MAGIC - %sh: Allows you to run shell code in your notebook
# MAGIC - %fs: Allows you to use dbutils filesystem commands
# MAGIC - %md: Allows you to write in markdown

# COMMAND ----------

# MAGIC %md ####List folders in Databricks File Store [(DBFS)](https://docs.databricks.com/data/databricks-file-system.html)

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %md ### List files in a folder in DBFS

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

# MAGIC %md 
# MAGIC ### List databases
# MAGIC Your data may already be available in a database, here is how to view all the databases in your workspace. We are using the %sql magic command to use SQL to show the databases.

# COMMAND ----------

# DBTITLE 1,List all databases
# MAGIC %sql
# MAGIC
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md There may be a lot of databases, you can refine the list by adding query parameters.

# COMMAND ----------

# DBTITLE 1,List databases based on criteria
# MAGIC %sql
# MAGIC
# MAGIC SHOW DATABASES LIKE 'aa*';

# COMMAND ----------

# MAGIC %md ### List tables within a database
# MAGIC
# MAGIC View tables in a specific database

# COMMAND ----------

# DBTITLE 1,List specific tables within a database
# MAGIC %sql 
# MAGIC
# MAGIC SHOW TABLES FROM aa_ba_msref_db_delta;

# COMMAND ----------

# MAGIC %md ### List csv files within a drive
# MAGIC
# MAGIC %md You can define a function to find specific file formats. In the cell below, we define a function get_csv_files and use it to look for all the csv files in the /FileStore/.

# COMMAND ----------

def get_csv_files(directory_path):
  """recursively list path of all csv files in path directory """
  csv_files = []
  files_to_treat = dbutils.fs.ls(directory_path)
  while files_to_treat:
    path = files_to_treat.pop(0).path
    if path.endswith('/'):
      files_to_treat += dbutils.fs.ls(path)
    elif path.endswith('.csv'):
      csv_files.append(path)
  return csv_files

#call the function by updating the path
get_csv_files('/FileStore/')  #if this errors remove the display()

# COMMAND ----------

# MAGIC %md The function above can be modified to find any file type. Copy the code and modify to your heart's content.
# MAGIC

# COMMAND ----------

# MAGIC %md ## 🔑 Read/Write Data by Data Source
# MAGIC
# MAGIC The cells in this section will not run without tweaking to your environment. This is by design. 

# COMMAND ----------

# MAGIC %md ###AWS S3 Bucket
# MAGIC
# MAGIC This is connecting to s3, with the assumption that the s3 is already mounted, based on the [docs](https://docs.databricks.com/data/data-sources/aws/amazon-s3.html)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Read from S3
path = "s3a:/example/location"
file_type = "csv" #Can be 'csv' , 'parquet', 'json' 

df = spark.read.format(file_type).option("inferSchema", "true").load(path)

# COMMAND ----------

# Write from a dataframe to S3 (CSV)
df.coalesce(1).write.format("com.databricks.spark.csv")
   .option("header", "true").save("s3a://my_bucket/example.csv")

# COMMAND ----------

# Write to DBFS (CSV)
df.write.save('/FileStore/parquet/example.csv', format='csv')


# COMMAND ----------

# DBTITLE 1,Standard JDBC
# MAGIC %scala
# MAGIC
# MAGIC // This standard connection has worked better using scala code versus python
# MAGIC
# MAGIC import java.sql.{Connection, DriverManager}
# MAGIC
# MAGIC val driver = "oracle.jdbc.driver.OracleDriver"
# MAGIC val url = "jdbc:oracle:thin:@//12.234.345.12:1521/db_current"
# MAGIC val username = "hadoopreaderdev"
# MAGIC val password = dbutils.secrets.get(scope = databricksScope, key = keyVaultSecret)
# MAGIC var connection:Connection = null
# MAGIC try {
# MAGIC   // make the connection
# MAGIC   Class.forName(driver)
# MAGIC   connection = DriverManager.getConnection(url, username, password)
# MAGIC   // create the statement, and run the select query
# MAGIC   val statement = connection.createStatement()
# MAGIC   val resultSet = statement.executeQuery("SELECT * FROM db.table")
# MAGIC   while ( resultSet.next() ) {
# MAGIC     val text = resultSet.getString(1)
# MAGIC     println(text)
# MAGIC   }
# MAGIC } catch {
# MAGIC   case e => e.printStackTrace
# MAGIC }
# MAGIC connection.close()

# COMMAND ----------

# MAGIC %md ## ✍️ Read/Write Data by generic Data Sources

# COMMAND ----------

# MAGIC %md ### Excel
# MAGIC You can load Excel files into Databricks, but extremely large files may have performance issues. Below we are using the [koalas library](https://koalas.readthedocs.io/en/latest/reference/api/databricks.koalas.read_excel.html) to load an excel file. You will also need to install two packages to load excel files.

# COMMAND ----------

# DBTITLE 1,Add Excel libraries
#%pip install koalas
#%pip install xlrd
#%pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Load Excel .xlsx with Python Koalas
# MAGIC %%time
# MAGIC # If you are using a legacy runtime (7.3, 9.1) uncomment and use Koalas, otherwise use pyspark pandas
# MAGIC import databricks.koalas as ks
# MAGIC
# MAGIC kdf = ks.read_excel('/mnt/<file>.xlsx')  
# MAGIC kdf.display(5)

# COMMAND ----------

# DBTITLE 1,Load Excel .xlsx with Pyspark Pandas
# MAGIC %%time
# MAGIC import pyspark.pandas as pd
# MAGIC pdp = pd.read_excel('/mnt/<file>.xlsx')
# MAGIC pdp.display(5)

# COMMAND ----------

# MAGIC %md Older excel files need to be loaded differently

# COMMAND ----------

# DBTITLE 1,Load Excel .xls with Python Koalas
# MAGIC %%time
# MAGIC from pyspark.sql.types import *
# MAGIC import databricks.koalas as ks
# MAGIC
# MAGIC # Enable Arrow-based columnar data transfers
# MAGIC spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
# MAGIC
# MAGIC df = ks.read_excel("/mnt/<file>.xls").to_spark() #import and change to sparkdf
# MAGIC
# MAGIC for col in df.columns:
# MAGIC      df = df.withColumn(col, df[col].cast(StringType())) #cast columns to string
# MAGIC     
# MAGIC df.display(5) #verify Schema  

# COMMAND ----------

# MAGIC %md Writing to excel requires a library, here we are using xlsxwriter. A package will need to be installed on the cluster with the following coordinates: "com.crealytics:spark-excel_2.12:0.13.5"

# COMMAND ----------

df.write.format("com.crealytics.spark.excel")\
  .option("header", "true")\
  .mode("overwrite")\
  .save(path)

# COMMAND ----------

# MAGIC %md Alternatively, there is a python native library that can be installed instead

# COMMAND ----------

# python
import xlsxwriter
from shutil import copyfile

workbook = xlsxwriter.Workbook('/dbfs/tmp/quick_start/rates.xlsx')
worksheet = workbook.add_worksheet()
worksheet.write(0, 0, "Key")
worksheet.write(0, 1, "Value")

copyfile('/dbfs/tmp/quick_start/rates.xlsx', '/dbfs/tmp/excel.xlsx')

# COMMAND ----------

# MAGIC %md ### Text

# COMMAND ----------

# DBTITLE 1,Load Text File with Python
df = spark.read.text("/FileStore/airport_timezones.txt")
df.display()

# COMMAND ----------

# MAGIC %md ### CSV

# COMMAND ----------

# DBTITLE 1,Loading CSV with Python
df = spark.read.csv("/tmp/quick_start/FullLoadFactor.csv", header="true", inferSchema="true")
df.display()

# COMMAND ----------

# DBTITLE 1,Loading CSV with R
# MAGIC %r
# MAGIC library(SparkR)
# MAGIC load_df <- read.df("/tmp/quick_start/FullLoadFactor.csv", source = "csv", header="true", inferSchema = "true")
# MAGIC display(load_df)
# MAGIC

# COMMAND ----------

# MAGIC %r
# MAGIC loads_by_indicator <- collect(sql(sqlContext, "select avg(LOAD_FACTOR) as AVG_LOAD_FACTOR, Indicator, Station from airline_load_factor group by Indicator"))

# COMMAND ----------

# MAGIC %r
# MAGIC library(ggplot2)
# MAGIC
# MAGIC p <- ggplot(airline_load_factor, aes(Indicator, LOAD_FACTOR))+ geom_point(alpha = 0.2) + facet_wrap(~Station)
# MAGIC p

# COMMAND ----------

# MAGIC %md ### Email
# MAGIC Send an email from a notebook

# COMMAND ----------

# MAGIC %pip install boto3

# COMMAND ----------

# Import the boto3 client
import boto3

# Set the AWS region name, retrieve the access key & secret key from dbutils secrets.
# For information about how to store the credentials in a secret, see
# https://docs.databricks.com/user-guide/secrets/secrets.html
AWS_REGION = "<region-name>"
ACCESS_KEY = dbutils.secrets.get('<scope-name>','<access-key>')
SECRET_KEY = dbutils.secrets.get('<scope-name>','<secret-key>')
sender='<sender@email-domain.com>'

#Create the boto3 client with the region name, access key and secret keys.
client = boto3.client('sns',region_name=AWS_REGION,
aws_access_key_id=ACCESS_KEY,
aws_secret_access_key=SECRET_KEY)

# Add email subscribers
for email in list_of_emails:
    client.subscribe(
        articleArn=article_arn,
        Protocol='email',
        Endpoint=email  # <-- email address who'll receive an email.
    )

# Add phone subscribers
for number in list_of_phone_numbers:
    client.subscribe(
        articleArn=article_arn,
        Protocol='sms',
        Endpoint=number  # <-- phone numbers who'll receive SMS.
    )

# Send message to the SNS article using publish method.
response = client.publish(
articleArn='<article-arn-name>',
Message="Hello From Databricks..",
Subject='Email Notification from Databricks..',
MessageStructure='string'
)

# COMMAND ----------

# MAGIC %md ### Zip Files to Spark with Python
# MAGIC
# MAGIC The following steps show how to pull csv files out of a zip files and load it into spark. It is spread over several cells because it uses python, %sh and %fs magic commands.
# MAGIC 1. Unzip the file.
# MAGIC 1. Remove first comment line.
# MAGIC 1. Remove unzipped file.

# COMMAND ----------

import urllib 
urllib.request.urlretrieve("https://resources.lendingclub.com/LoanStats3a.csv.zip", "/tmp/LoanStats3a.csv.zip")

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /tmp/LoanStats3a.csv.zip
# MAGIC tail -n +2 LoanStats3a.csv > temp.csv
# MAGIC rm LoanStats3a.csv

# COMMAND ----------

# DBTITLE 1,Move temp file to DBFS- load into dataframe
dbutils.fs.mv("file:/databricks/driver/temp.csv", "dbfs:/mnt/Hayley/LendingClubCsv/temp.csv")  

df = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("dbfs:/mnt/Hayley/LendingClubCsv/temp.csv")
display(df)

# COMMAND ----------

# MAGIC %md ## *️⃣ Other Tasks

# COMMAND ----------

# MAGIC %md ### Data Exploration with SQL
# MAGIC
# MAGIC Here is a simple visual using the built in visualization tool within Databricks. [Here is a notebook you can import to try additional visualizations](https://docs.databricks.com/_static/notebooks/charts-and-graphs-python.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE or REPLACE TEMPORARY VIEW airline_load_factor
# MAGIC USING CSV
# MAGIC OPTIONS (path "/tmp/quick_start/FullLoadFactor.csv", header "true", mode "FAILFAST");
# MAGIC
# MAGIC SELECT avg(LOAD_FACTOR) as avg_load_factor, Indicator, Station from airline_load_factor group by Indicator, Station order by avg_load_factor desc;

# COMMAND ----------

# MAGIC %md ### Schedule a Job in Databricks
# MAGIC
# MAGIC [Workflows!](https://databricks.com/blog/2022/05/10/introducing-databricks-workflows.html)
# MAGIC
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2022/05/db-154-img-1b.jpg"  width="300">
# MAGIC

# COMMAND ----------

import databricks

#Create object to communicate with API
db = databricks.API(environment='prod', workspace='abde')

# COMMAND ----------

# DBTITLE 1,Clusters
#List all clusters
all_clusters = db.Clusters.get_list() #use .get_df if you want a dataframe
#List all clusters that contain string
filtered_clusters = db.Clusters.get_list('MyGroup') #use .get_df if you want a dataframe
#Get cluster config
cluster_config = db.Clusters.get_cluster_config('MyGroup-abcd')

# COMMAND ----------

# DBTITLE 1,Jobs
#List all jobs
all_jobs = db.Jobs.get_list() #use .get_df if you want a dataframe
#List all jobs that contain string
filtered_jobs = db.Jobs.get_list('MyGroup') #use .get_df if you want a dataframe
#Get job config
job_config = db.Jobs.get_job_config('My_job')

# COMMAND ----------

# DBTITLE 1,Schedule a job
#Schedule a new job (and give default permissions)
scheduled_job = db.Jobs.schedule_new_job(job_config) #Ask me what job_config looks like
#Change permissions
db.Jobs.change_permissions(job_id,permissions_config) #Ask me what permissions_confg looks like

# COMMAND ----------

# DBTITLE 1,Delete a job
#Delete a job given an id
db.Jobs.delete_job(job_id)

# COMMAND ----------

# MAGIC %md ### Databricks SQL
# MAGIC
# MAGIC Not covered in this notebook, in the upper left corner, select [S] for [Databricks SQL, a simple sql based interface](https://docs.microsoft.com/en-us/azure/databricks/sql/) for SQL users who want to run quick ad-hoc queries on their data. Use native connectors with SQL endpoints to easily connect, and optimize the performance of data to your favorite BI tool.

# COMMAND ----------

# MAGIC %md ### Machine Learning
# MAGIC
# MAGIC Not covered in this notebook, in the upper left corner, select [M] for [Machine Learning](https://docs.databricks.com/applications/machine-learning/index.html#databricks-machine-learning-overview) to use your data for machine learning or data science projects, and use the built in Machine Learning functionality like feature store, AutoML, and MLFLow model management. Use a [Databricks ML runtime](https://docs.databricks.com/runtime/mlruntime.html), which comes preloaded with popular open source ML libraries like Sklearn, TensorFlow, PyTorch, Keras, and XGBoost.

# COMMAND ----------

# MAGIC %md ### Merge data sets
# MAGIC
# MAGIC Combine 2 data sets, one from a text file and one from the database.

# COMMAND ----------

# Two State Code Lookup
df_state_lookup= spark.read.format('csv').options(header='true', delimiter='|').load('/tmp/quick_start/State_Reference.txt')
df_state_lookup.display()


# COMMAND ----------

# mix case a column with a sql function
from pyspark.sql.functions import initcap, col

df_state_lookup= df_state_lookup.select(initcap(col('state')).alias("State"),col('State_Code'))

# create a view
df_state_lookup.createOrReplaceTempView("StateLookup")

# COMMAND ----------

# Create freight dataset
freight = spark.sql("""select ORIGIN_STATE_NM, round(avg(CAST(data_struct.domestic_market.FREIGHT as INT)),0) as Freight from data_struct.domestic_market where FREIGHT > 0 group by ORIGIN_STATE_NM;"""
)
freight.createOrReplaceTempView("freight")
display(freight)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select a.State_Code, Freight from StateLookup a, freight b where a.State = b.ORIGIN_STATE_NM order by Freight desc limit 5

# COMMAND ----------

# MAGIC %md Joining two tables with SQL
# MAGIC
# MAGIC Note: This is from a different workspace and will not run in the databricks academy. Provided for syntax

# COMMAND ----------


  rtpq3 = spark.sql (  
"""select 
A.*, 
b.MKT_TYPE as DOM_MKT_TYPE
,case when c.pcc =1 then 'PCC' else 'NON-PCC' end as PCC
from rtpq2 A
left join mkts b
on A.markets = b.STATION_OD
left join pcc c 
on A.lylty_acct_id = c.lylty_acct_id
 """ )
rtpq3.createOrReplaceTempView("rtpq3")
##display(rtpq3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### HTML in notebooks

# COMMAND ----------

displayHTML("<h1><marquee>Document your code w/ displayHTML function!</marquee></h1>")
import time
import math
programStartTime = time.time()

# COMMAND ----------

programEndTime = time.time()
runTime = programEndTime - programStartTime
strHours = str(math.floor(runTime / 3600.0))
strMinutes = ("00" + str(math.floor(math.floor(runTime / 60.0)) % 60))[-2:]
strSeconds = ("00" + str(math.floor(runTime % 60.0)))[-2:]

displayString = "Total Runtime is: " + str(strHours) + ":" + str(strMinutes) + ":" + str(strSeconds)
displayHTML("""
<b>
  <font size="8" color="#4CAF50" face="sans-serif">
    {}
  </font>
</b>
</br></br>
""".format(displayString))

# COMMAND ----------

# MAGIC %md ### Markdown to comment your code

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Header level 1
# MAGIC
# MAGIC ## Header level 2
# MAGIC
# MAGIC ### Header level 3
# MAGIC
# MAGIC Regular text
# MAGIC
# MAGIC *Italic text*
# MAGIC
# MAGIC **Bold text**
# MAGIC
# MAGIC ***Bold and Italic text***
# MAGIC
# MAGIC Bulleted List
# MAGIC * item element
# MAGIC   * subitem
# MAGIC   * subitem
# MAGIC * item element
# MAGIC * item element
# MAGIC
# MAGIC Numeric List
# MAGIC 1. item element
# MAGIC 2. item element
# MAGIC 3. item element
# MAGIC
# MAGIC Inline code chunk: `print('Hello world')`
