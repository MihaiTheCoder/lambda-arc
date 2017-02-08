import AssemblyKeys._
// -----------------------------
// project definition
// -----------------------------

name := "Spark Lambda"

organization := "com.qubiz.bigdata"
scalaVersion := "2.11.8"
// -----------------------------
// common dependencies
// -----------------------------

val sparkVersion = "1.6.3"
val chillVersion = "0.7.2"
val algebirdVersion = "0.11.0"
val kafkaClientsVersion = "0.8.2.1"
val avroVersion = "1.7.7"
val sparkCassandraConnectorVersion = "1.6.1"
val cassandraDriverCoreVersion = "3.0.1"
val nScalaTimeVersion = "2.6.0"
val comTypesafeVersion = "1.3.0"
val slf4jVersion = "1.7.22"
val provided = "compile"//"provided"


// -----------------------------
// Add your stuff here
// -----------------------------

mainClass in Compile := Some("streaming.StreamingJob")
jarName in assembly := "spark-lambda.jar"

mergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

libraryDependencies += "com.typesafe" % "config" % comTypesafeVersion
// https://mvnrepository.com/artifact/com.github.nscala-time/nscala-time_2.11
libraryDependencies += "com.github.nscala-time" % "nscala-time_2.11" % nScalaTimeVersion
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.11
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % sparkCassandraConnectorVersion
// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverCoreVersion
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % slf4jVersion
// https://mvnrepository.com/artifact/com.twitter/chill_2.11
libraryDependencies += "com.twitter" % "chill_2.11" % chillVersion
// https://mvnrepository.com/artifact/com.twitter/chill-avro_2.11
libraryDependencies += "com.twitter" % "chill-avro_2.11" % chillVersion
// https://mvnrepository.com/artifact/com.twitter/algebird-core_2.11
libraryDependencies += "com.twitter" % "algebird-core_2.11" % algebirdVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion % provided

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % kafkaClientsVersion

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion % provided

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % provided
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % sparkVersion


libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-assembly_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % sparkVersion % provided

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % sparkVersion % provided

libraryDependencies += "joda-time" % "joda-time" % "2.8.1"

libraryDependencies += "org.joda" % "joda-convert" % "1.7"

libraryDependencies += "commons-io" % "commons-io" % "2.4"
