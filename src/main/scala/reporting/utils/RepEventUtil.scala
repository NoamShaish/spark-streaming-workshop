package reporting.utils

/**
 * Created by noams on 2/2/15.
 */
object RepEventUtil {
  def getField(eventLine: String, index: Int) = eventLine.split(" ")(index)

  def getBodyField(eventLine: String, index: Int) = {
    val fields: Array[String] = eventLine.split(" ")
    fields(index + fields(0).toInt)
  }

  def isChatStatusChanged(eventLine: String): Boolean =
  List("1", "3", "4").contains(getType(eventLine))

  def isConnectedChatEvent(eventLine: String): Boolean =
  isRepEvent(eventLine) && isChatStatusChanged(eventLine)

  def isRepEvent(eventLine: String): Boolean =
  getEventType(eventLine) == "RepSessionEvent" || getEventType(eventLine) == "RepActivityEvent"

  def getEventType(eventLine: String) = getField(eventLine, 2)

  def getType(eventLine: String): String = getBodyField(eventLine, 4)

  def getSubType(eventLine: String) = getBodyField(eventLine, 5)

  def getPrevSubType(eventLine: String) = getBodyField(eventLine, 9)

  def getConcurrentEng(eventLine: String) = getBodyField(eventLine, 17)

  def getPrevConcurentEng(eventLine: String) = getBodyField(eventLine, 16)

  def getEventTime(eventLine: String) = getBodyField(eventLine, 6)

  def getPrevRepActivityEventTime(eventLine: String) = getBodyField(eventLine, 10)

  def getPrevRepSessionEventTime(eventLine: String) = getBodyField(eventLine, 8)
}
