name := "spark-streaming"

version := "1.0"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Maven Repository" at "http://central.maven.org/maven2/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.1.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.1.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.1.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4" ,
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4" classifier "models",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0",
  "javax.servlet" % "javax.servlet-api" % "3.0.1"
)