package sentiment.utils

/**
 * Created by noams on 12/18/14.
 */
object PLineUtil {
  /**
   * @param line - input PLine
   * @return chat fraction of PLine
   */
  def getChat(line: String) = line.split(" \"|\" ")(15).replaceAll("\"", "")
}
