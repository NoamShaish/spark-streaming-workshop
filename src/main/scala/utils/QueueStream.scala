package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.io.Source

/**
 * Created by noams on 1/29/15.
 */
object QueueStream {

  // An RDD mutable queue to provide input for computation
  val rddQueue = new mutable.SynchronizedQueue[RDD[String]]

  // Create stream out of queue
  def getStream(context: StreamingContext): DStream[String] = context.queueStream(rddQueue)

  // Push PLines into input queue
  def pushData(context: StreamingContext, fileName: String): Unit = {
    // Read source PLines
    def source = Source.fromURL(getClass.getResource(fileName))
    def lines: Iterator[String] = source.getLines()

    // Split file to 100 batches
    val subListSize: Int = lines.size % 100
    val sliding = lines.sliding(subListSize)

    // Enqueue batch with a small delay to mock stream behavior
    while (sliding.hasNext) {
      rddQueue += context.sparkContext.makeRDD(sliding.next(), 10)
      Thread.sleep(10)
    }
  }
}
