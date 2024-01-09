// Databricks notebook source
// MAGIC %md 
// MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/smolder-solacc.git. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/hl7v2.

// COMMAND ----------

// MAGIC %md
// MAGIC # Smolder: A package for ingesting HL7 messages to Delta Lake
// MAGIC
// MAGIC Ingesting HL7 and/or FIHR data is a common pattern/use case in Healthcare 
// MAGIC
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC <img src="https://databricks.com/wp-content/uploads/2021/01/blog-image-ehr-in-rt-1.jpg" width=900>
// MAGIC
// MAGIC The general workflow is you’ll have messages coming from an interface engine, often times people might put it into something like a Kafka or EventHubs - and you can use a tool like Smolder to process those and help you build out this medallion architecture more easily.  
// MAGIC - Having these bronze, silver, gold layers are important because what we oftentimes see is, an HL7 feed might have, (for example) a table of 80 key messages, and from that you might start building your silver and gold layers
// MAGIC - Sometimes the logic changes and you need to go back and reprocess those messages, or the message fields might not be populated the same way across institutions. Although it’s a standard, it’s a standard that’s often violated, and so you may need to go back and reprocess. 
// MAGIC - So keeping these raw tables has been really successful for a lot of our HLS customers, so they can actually go back if some particular hospital/customer/whatever is violating and they need to do some change to business logic
// MAGIC - That’s why for this demo, we’ll be building out this Medallion Architecture with incoming HL7 data - a common use case in healthcare
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Cluster Set Up
// MAGIC
// MAGIC * Make sure [the Smolder jar file](https://amir-hls.s3.us-east-2.amazonaws.com/public/263572c0_25a1_46ce_9009_2ae456966ea9-smolder_2_12_0_0_1_SNAPSHOT-615ef.jar) is attached to your cluster: If you run the `RUNME` file in this folder, the cluster setup is automated and a Workflow and a `smolder_cluster` is created for you. Feel free to try running this notebook using the Workflow. Alternatively, install [the Smolder jar file](https://amir-hls.s3.us-east-2.amazonaws.com/public/263572c0_25a1_46ce_9009_2ae456966ea9-smolder_2_12_0_0_1_SNAPSHOT-615ef.jar) to the `smolder_cluster` cluster and attach this notebook to run interactively.
// MAGIC * If you run this notebook with the "Run All" option, the last block terminates the streams for you. If you opt to run this notebook block by block, *make sure to cancel your streaming commands, otherwise your cluster will not autoterminate*. 

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS jo_catalog.demos

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load HL7 messages to a DataFrame _with streaming_
// MAGIC
// MAGIC Now we can start streaming from systems like Kafka, Event Hub, Kinesis, etc. because we have access to all the connectors that Spark has access to. By working with Smolder, we get all the Spark goodness and enhanced performance that comes with using Spark as a wrapper. 
// MAGIC
// MAGIC **Now let's read hl7 messages as a spark stream:** 
// MAGIC - We're using all the spark internals 
// MAGIC - We're using `readStream` for hl7 and pointing it to our source location (a folder with HL7 data)
// MAGIC - We've set it to 100 files per trigger, this can be adjusted
// MAGIC - `readStream` = reading as stream instead of batch

// COMMAND ----------

val schema = spark.read.format("hl7").load("/databricks-datasets/rwe/ehr/hl7").schema

val message_stream = spark.readStream.format("hl7") 
  .schema(schema)
  .option("maxFilesPerTrigger", "100")
  .load("/databricks-datasets/rwe/ehr/hl7") 

message_stream.printSchema()

// COMMAND ----------

val messages_df = spark.read.format("hl7").load("/databricks-datasets/rwe/ehr/hl7")
display(
  messages_df.select(explode(col("segments")).alias("segments"))
  .where(col("segments.id").like("PID"))
)

// COMMAND ----------

// MAGIC %md
// MAGIC Smolder creates this really nice `segments` column - and being able to declare the coordinates for your HL7 messages and provide a mapping, makes it easy to go back and reprocess data if there's any discrepancy (which happens a lot with healthcare data). We can easily solve this problem with a Smolder + Databricks solution.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Define HL7 helper functions
// MAGIC
// MAGIC HL7 uses an interesting set of delimiters to split records. These are helper functions for working with HL7 messages.

// COMMAND ----------

// DBTITLE 1,We can use helper functions to create our DataFrame of ADT Events.
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

def segmentField(segment: String, field: Int): Column = {
  expr("filter(segments, s -> s.id == '%s')[0].fields".format(segment)).getItem(field)
}

def subfield(col: Column, subfield: Int): Column = {
  split(col, "\\^").getItem(subfield)
}

// COMMAND ----------

val adtEvents = message_stream.select(subfield(segmentField("PID", 4), 0).as("lastName"),
                                subfield(segmentField("PID", 4), 1).as("firstName"),
                                segmentField("PID", 2).as("patientID"),
                                segmentField("EVN", 0).as("eventType"),
                                subfield(segmentField("PV1", 2), 3).as("hospital"))

adtEvents.createOrReplaceTempView("adt_events")

// COMMAND ----------

// MAGIC %md
// MAGIC # Use streaming SQL queries to interact with our HL7 stream
// MAGIC Now we can start writing live SQL queries on the data as it's coming in. So this is simulating a stream like one would have with Kafka.
// MAGIC Using the magic command `%sql` we can switch languages inside our notebook and begin analyzing our data in near real-time. 

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Find high utilizers
// MAGIC Now, we'll extract the patient identifiers and hospitals that people are visiting, to get a visualization showing high utilizers. This displays a simple visualization of high utilizers by hospital. And we can inspect things like: records processed, batch size, etc.
// MAGIC
// MAGIC
// MAGIC We can also build visualizations that help us identify in real time where we have high-utilization patients across our hospital system.

// COMMAND ----------

// DBTITLE 1,See High Utilizers (By Hospital)
// MAGIC %sql
// MAGIC SELECT 
// MAGIC COUNT(eventType) as event_count
// MAGIC , eventType
// MAGIC , patientID
// MAGIC , firstName
// MAGIC , hospital from adt_events 
// MAGIC GROUP BY hospital, eventType, patientID, firstName, lastName
// MAGIC ORDER BY event_count DESC
// MAGIC Limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC COUNT(eventType) as event_count
// MAGIC , patientID
// MAGIC , firstName
// MAGIC  from adt_events 
// MAGIC GROUP BY patientID, firstName
// MAGIC ORDER BY event_count DESC

// COMMAND ----------

// MAGIC %md
// MAGIC # Dashboarding
// MAGIC We can utilize Databrick's built-in dashboarding tools that provide a quick and easy option if you don't want to bother connecting to a 3rd party tool. Alternatively, we integrate easily with 3rd party tools if preferred (via Partner Connect) and throughout the platform. 

// COMMAND ----------

// DBTITLE 1,Select our catalog
// MAGIC %sql
// MAGIC USE CATALOG jo_catalog

// COMMAND ----------

// DBTITLE 1,Select our database
// MAGIC %sql
// MAGIC USE DATABASE demos

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS hl7_adt_stream2

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE hl7_adt_stream2
// MAGIC USING delta

// COMMAND ----------

val output_path = "dbfs:/tmp/HL7_demo/bronze_delta"
val checkpoint_path = "dbfs:/tmp/HL7_demo/checkpoint/" 
val bad_records_path = "dbfs:/tmp/HL7_demo/bronze_delta/badRecordsPath/"

dbutils.fs.mkdirs(output_path)
dbutils.fs.rm(output_path,true) 
dbutils.fs.mkdirs(checkpoint_path)
dbutils.fs.rm(checkpoint_path,true) 
dbutils.fs.mkdirs(bad_records_path)
dbutils.fs.rm(bad_records_path,true) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Persist Our Stream to Delta
// MAGIC **Now you can write HL7 stream data to delta bronze layer:**
// MAGIC
// MAGIC Now we're going to write this out to a Delta table. We have a ProcessingTime of every 5 seconds, so every 5 seconds it will write out the state of our particular streaming batch. You could write TriggerOnce if you want to run this as a batch job, and it's this flexibility of running batch and stream without making major code modifications is one of the reasons why a lot of our customers have moved towards the streaming paradigm and using an integration like this. 

// COMMAND ----------

// DBTITLE 1,Trigger set to 5 seconds (near real-time), so you can see the refresh
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame

val query = adtEvents
            .writeStream
            .outputMode("append")
            .format("delta")
            .option("mergeSchema", "true")
            .option("checkpointLocation", checkpoint_path)
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
              batchDF.write.format("delta").mode("append")
                     .option("mergeSchema", "true")
                     .saveAsTable("hl7_adt_stream2")
            }
            .start()
Thread.sleep(120000)

// COMMAND ----------

// DBTITLE 1,Look at the table history to see what's been going on
// MAGIC %sql
// MAGIC DESC HISTORY hl7_adt_stream2
// MAGIC
// MAGIC -- Since we're using Delta, we've got a full audit trail of all the streaming updates, full table versions, etc. so if you're doing a merge or update on the table, if you ever need to go back and see the state of the table at any given time, you'll have that flexibility very easily with Delta

// COMMAND ----------

// DBTITLE 1,Run a count(*) on our current view
// MAGIC %sql
// MAGIC SELECT count(*) FROM hl7_adt_stream2
// MAGIC
// MAGIC -- We can run a count on our current state to see how many records are in the table.

// COMMAND ----------

// DBTITLE 1,R
// MAGIC %sql
// MAGIC SELECT count(*) FROM hl7_adt_stream2@v2
// MAGIC
// MAGIC -- If we want to see how many records were in the table at a certain point in history, we can either use the timestamp or version to achieve this. This example uses a cool shorthand trick to select version 2 of the table

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hl7_adt_stream2@v2

// COMMAND ----------

// MAGIC %md
// MAGIC ## Recap
// MAGIC ##### What we saw:
// MAGIC * Streaming HL7 ingestion & processing
// MAGIC * Near real-time query and analytics of streaming data
// MAGIC * Easily stream into a persisted Delta table
// MAGIC * Leverage Delta functionality for auditability and reproducibility 
// MAGIC
// MAGIC Now let's gracefully terminate the streaming queries after a few minutes.

// COMMAND ----------

// Thread.sleep(120000)
// for (s <- spark.streams.active) {
//   s.stop
// }

// COMMAND ----------

// MAGIC %md
// MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
// MAGIC
// MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
// MAGIC | :-: | :-:| :-: | :-:|
// MAGIC | Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
// MAGIC |Smolder |Apache-2.0 License| https://github.com/databrickslabs/smolder | https://github.com/databrickslabs/smolder/blob/master/LICENSE|

// COMMAND ----------

// MAGIC %md
// MAGIC ## Disclaimers
// MAGIC Databricks Inc. (“Databricks”) does not dispense medical, diagnosis, or treatment advice. This Solution Accelerator (“tool”) is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information (“PHI”) as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account. Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.
// MAGIC
// MAGIC All names have been synthetically generated, and do not map back to any actual persons or locations
