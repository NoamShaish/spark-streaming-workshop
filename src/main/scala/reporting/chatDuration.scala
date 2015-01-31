package reporting

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by noams on 1/29/15.
 */
object chatDuration {
  def getField(eventLine: String, index: Int) = eventLine.split(" ")(index)

  def getEventType(eventLine: String) = getField(eventLine, 2)

  def isChatStatusChanged(eventLine: String): Boolean = List("1", "3", "4").contains(getType(eventLine))

  def getType(eventLine: String): String = getField(eventLine, 6)

  def isConnectedChatEvent(eventLine: String): Boolean = isRepEvent(eventLine) && isChatStatusChanged(eventLine)

  def isRepEvent(eventLine: String): Boolean = getEventType(eventLine) == "RepSessionEvent" || getEventType(eventLine) == "RepActivityEvent"

  def getSubType(eventLine: String) = getField(eventLine, 7)

  def getPrevSubType(eventLine: String) = getField(eventLine, 11)

  def getConcurrentEng(eventLine: String) = getField(eventLine, 19)

  def getPrevConcurentEng(eventLine: String) = getField(eventLine, 18)

  def getEventTime(eventLine: String) = getField(eventLine, 8)

  def calculateDuration(eventLine: String) = getEventTime(eventLine)

  /**
   * Main entry to application
   * @param args - application arguments
   */
  final def main (args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Chat Duration")
    sparkConf.setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val context = new StreamingContext(sparkConf, Seconds(1))
    context.textFileStream(getClass.getResource("../reportEvents.txt").getPath)
      .filter(row => isConnectedChatEvent(row))
      .map(eventLine => (getEventType(eventLine), calculateDuration(eventLine), "0", getSubType(eventLine),
      getPrevSubType(eventLine), getConcurrentEng(eventLine), getPrevConcurentEng(eventLine))).print()


    context.start()
    context.awaitTermination()
  }
}
