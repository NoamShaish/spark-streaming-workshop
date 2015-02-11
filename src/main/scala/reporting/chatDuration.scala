package reporting

import _root_.utils.QueueStream
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import reporting.utils.{RepEventUtil}

/**
 * Created by noams on 1/29/15.
 */
object ChatDuration {

  def calculateDuration(eventLine: String) = {
    val prevEventTime: Long = Math.max(RepEventUtil.getPrevRepSessionEventTime(eventLine).toLong, RepEventUtil.getPrevRepActivityEventTime(eventLine).toLong)
    if (prevEventTime < 0) 0l else RepEventUtil.getEventTime(eventLine).toLong - prevEventTime
  }

  def parseEvent(eventLine: String) = (RepEventUtil.getEventType(eventLine), calculateDuration(eventLine), "0", RepEventUtil.getSubType(eventLine),
      RepEventUtil.getPrevSubType(eventLine), RepEventUtil.getConcurrentEng(eventLine), RepEventUtil.getPrevConcurentEng(eventLine))

  /**
   * Main entry to application
   * @param args - application arguments
   */
  final def main (args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Chat Duration")
    sparkConf.setMaster("local[10]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val context = new StreamingContext(sparkConf, Seconds(1))

    QueueStream.getStream(context)
      .filter(row => RepEventUtil.isConnectedChatEvent(row))
      .map(parseEvent).print()


    context.start()
    QueueStream.pushData(context, "../reportEvents.txt")
    context.awaitTermination()
  }
}
