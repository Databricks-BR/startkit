# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

# COMMAND ----------

# MAGIC %md 
# MAGIC # Read data from Apache Kafka

# COMMAND ----------

df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "<server:ip>")
  .option("subscribe", "<topic>")
  .option("startingOffsets", "latest")
  .load()
)

# COMMAND ----------

# MAGIC %md # Write to a Delta table

# COMMAND ----------

(df.writeStream
  .option("checkpointLocation", "<checkpoint_path>")
  .toTable("<table_name>")`
)

# COMMAND ----------

# MAGIC %md # Write to a Kafka Sink

# COMMAND ----------

(df.writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "<server:ip>")
  .option("topic", "<topic>")
  .option("checkpointLocation", "<checkpoint_path>")
  .start()
)
