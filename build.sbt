name := "Stream Handler"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
	"org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
	"org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1" % "provided",
	"com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0",
	"com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0"
)

