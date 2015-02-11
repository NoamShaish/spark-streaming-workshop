package sentiment

import _root_.utils.QueueStream
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by noams on 12/22/14.
 */
object SentimentsStream {


  /**
   * Main entry to application
   * @param args - application arguments
   */
  final def main (args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Sentiment example")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.cassandra.connection.host", "127.0.0.1")

    // Create the context
    val context = new StreamingContext(sparkConf, Seconds(1))
    context.checkpoint("..")

    new StreamComputation(context).compute(QueueStream.getStream(context))
      .reduceByKeyAndWindow((x: Int,y: Int) => x + y, (x: Int,y: Int) => x - y, Seconds(5), Seconds(2))
      .saveToCassandra("sparkstreaming", "sentimentCount")

    context.start()
    QueueStream.pushData(context, "../allPlines.txt")
    context.awaitTermination()

  }
}
