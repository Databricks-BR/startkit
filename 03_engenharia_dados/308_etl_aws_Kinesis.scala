// Databricks notebook source
// MAGIC %md
// MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/startkit/main/images/startkit_logo.png">

// COMMAND ----------

// MAGIC %md #Kinesis WordCount
// MAGIC
// MAGIC ### This is a WordCount example with the following
// MAGIC - Kinesis as a Structured Streaming Source
// MAGIC - Stateful operation (groupBy) to calculate running counts of words
// MAGIC

// COMMAND ----------

// === Configurations for Kinesis streams ===
// If you are using IAM roles to connect to a Kinesis stream (recommended), you do not need to set the access key and the secret key
val awsAccessKeyId = "YOUR ACCESS KEY ID"
val awsSecretKey = "YOUR SECRET KEY"
val kinesisStreamName = "YOUR STREAM NAME"
val kinesisRegion = "YOUR REGION" // e.g., "us-west-2"

// COMMAND ----------

import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import java.nio.ByteBuffer
import scala.util.Random

// Verify that the Kinesis settings have been set
require(!awsAccessKeyId.contains("YOUR"), "AWS Access Key has not been set")
require(!awsSecretKey.contains("YOUR"), "AWS Access Secret Key has not been set")
require(!kinesisStreamName.contains("YOUR"), "Kinesis stream has not been set")
require(!kinesisRegion.contains("YOUR"), "Kinesis region has not been set")


// COMMAND ----------

// DBTITLE 1,Structured Streaming Query that reads words from Kinesis and counts them up
val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", kinesisStreamName)
  .option("region", kinesisRegion)
  .option("initialPosition", "TRIM_HORIZON")
  .option("awsAccessKey", awsAccessKeyId)
  .option("awsSecretKey", awsSecretKey)
  .load()
// val result = kinesis.selectExpr("lcase(CAST(data as STRING)) as word")
//   .groupBy($"word")
//   .count()
// display(result)

// COMMAND ----------

// DBTITLE 1,Write words to Kinesis
// Create the low-level Kinesis Client from the AWS Java SDK.
val kinesisClient = AmazonKinesisClientBuilder.standard()
  .withRegion(kinesisRegion)
  .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKeyId, awsSecretKey)))
  .build()

println(s"Putting words onto stream $kinesisStreamName")
var lastSequenceNumber: String = null

for (i <- 0 to 10) {
  val time = System.currentTimeMillis
  // Generate words: fox in sox
  for (word <- Seq("Through", "three", "cheese", "trees", "three", "free", "fleas", "flew", "While", "these", "fleas", "flew", "freezy", "breeze", "blew", "Freezy", "breeze", "made", "these", "three", "trees", "freeze", "Freezy", "trees", "made", "these", "trees", "cheese", "freeze", "That's", "what", "made", "these", "three", "free", "fleas", "sneeze")) {
    val data = s"$word"
    val partitionKey = s"$word"
    val request = new PutRecordRequest()
        .withStreamName(kinesisStreamName)
        .withPartitionKey(partitionKey)
        .withData(ByteBuffer.wrap(data.getBytes()))
    if (lastSequenceNumber != null) {
      request.setSequenceNumberForOrdering(lastSequenceNumber)
    }    
    val result = kinesisClient.putRecord(request)
    lastSequenceNumber = result.getSequenceNumber()
  }
  Thread.sleep(math.max(10000 - (System.currentTimeMillis - time), 0)) // loop around every ~10 seconds 
}

// COMMAND ----------

// DBTITLE 1,Write to a Delta table
kinesis.writeStream
  .format("delta")
  .option("checkpointLocation", "/tmp/kinesis-demo/_checkpoint")
  .table("kinesis_demo")
