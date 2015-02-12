package sentiment

import sentiment.utils.{PLineUtil, StanfordUtil}
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._

/**
 * Actual computation is irrelevant to stream type 
 * Created by noams on 12/16/14.
 */
class StreamComputation(context: StreamingContext) extends Serializable {
  /**
   * Single StanfordUtil broadcast across Spark nodes
   */
  val standfordUtil = context.sparkContext.broadcast(new StanfordUtil())

  /**
   * The computation logic is:
   *  1. Strip PLine to chat line
   *  2. flatMap each chat line to annotated sentences
   *  3. map each sentence to tuple ('sentiment score', 1)
   *  4. reduce by 'sentiment score' to get count per 'sentiment score'
   * @param inputStream - Input stream to compute
   * @return stream containing sentiment to number of occurrences mapping
   */
  def compute(inputStream: DStream[String]) : DStream[(Int, Int)] = {
    val flattenMap: DStream[CoreMap] =
      inputStream.flatMap(line => standfordUtil.value.getSentences(PLineUtil.getChat(line)))

    val mappedStream =
      flattenMap.map(sentence => (standfordUtil.value.getSentiment(sentence), 1))

    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream
  }
}
