import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import com.datastax.oss.driver.api.core.uuid.Uuids // com.datastax.cassandra:cassandra-driver-core:4.0.0
import com.datastax.spark.connector._              // com.datastax.spark:spark-cassandra-connector_2.11:2.4.3

case class DeviceData(payment: String, click: Double)

object StreamHandler {
	def main(args: Array[String]) {

		// initialize Spark
		val spark = SparkSession // the sparksession comes from org.apache.spark.sql._
			.builder
			.appName("Stream Handler")
			.config("spark.cassandra.connection.host", "localhost")
			.getOrCreate()

		import spark.implicits._

		// read from Kafka
		val inputDF = spark
			.readStream
			.format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("subscribe", "portfolio_click") // subscribe the topic
			.load()

		// the raw table only contains one column which is value
		// only select 'value' from the table,
		// convert from bytes to string
		val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

		// split each row on comma, load it to the case class
		// value --> "123, abcd, 234, 456"
		val expandedDF = rawDF.map(row => row.split(","))
			.map(row => DeviceData(
				row(1),
				row(2).toDouble
			))

		// groupby and aggregate
		val summaryDf = expandedDF
			.groupBy("payment")
			.agg(avg("click"))

		// create a dataset function that creates UUIDs
		val makeUUID = udf(() => Uuids.timeBased().toString)

		// add the UUIDs and renamed the columns
		// this is necessary so that the dataframe matches the 
		// table schema in cassandra
		val summaryWithIDs = summaryDf.withColumn("uuid", makeUUID())
			.withColumnRenamed("avg(click)", "click")
		

		// write dataframe to Cassandra
		val query = summaryWithIDs
			.writeStream
			.trigger(Trigger.ProcessingTime("5 seconds"))
			.foreachBatch { (batchDF: DataFrame, batchID: Long) =>
				println(s"Writing to Cassandra $batchID")
				batchDF.write
					.cassandraFormat("payment_recovery", "paymentspace") // table, keyspace
					.mode("append")
					.save()
			}
			.outputMode("update")
			.start()

		// until ^C
		query.awaitTermination()
	}
}