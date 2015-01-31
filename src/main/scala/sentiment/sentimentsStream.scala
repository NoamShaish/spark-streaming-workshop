package sentiment

import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by noams on 12/22/14.
 */
abstract class sentimentsStream {

  /**
   * @return name of Spark application
   */
  def getAppName(): String

  /**
   * @param context - Spark context
   * @return input stream
   */
  def getStream(context: StreamingContext): DStream[String]

  /**
   * Push data to be consumed by Spark
   * @param context - Spark context
   */
  def pushData(context: StreamingContext): Unit

  /**
   * Main entry to application
   * @param args - application arguments
   */
  final def main (args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(getAppName())
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.cassandra.connection.host", "127.0.0.1")

    // Create the context
    val context = new StreamingContext(sparkConf, Seconds(1))

    new StreamComputation(context).compute(getStream(context))
      .reduceByKeyAndWindow((x: Int,y: Int) => x + y, (x: Int,y: Int) => x - y, Seconds(5), Seconds(2))
      .saveToCassandra("sparkstreaming", "sentimentCount")

    context.start()
    pushData(context)
    context.awaitTermination()

  }
}
