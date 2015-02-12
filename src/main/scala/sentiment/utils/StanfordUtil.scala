package sentiment.utils

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.AnnotatedTree
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.SparkContext

import scala.collection.JavaConversions

/**
 * Created by noams on 12/18/14.
 */
class StanfordUtil() {
  /**
   * Stanford sentiment properties
   */
  private val properties = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  /**
   * Stanford core pipeline annotator
   */
  private val pipeline = new StanfordCoreNLP(properties)

  /**
   * @param line - Input chat line
   * @return lines annotations
   */
  private def getAnnotation(line: String) = {
    val annotation = new Annotation(line)
    pipeline.annotate(annotation)
    annotation
  }

  /**
   * @param sentence - Input sentence
   * @return annotation tree of given sentence
   */
  private def getTree(sentence: CoreMap) = sentence.get(classOf[AnnotatedTree])

  /**
   * Since Stanford library is a Java library, a conversion to scala collection is needed.
   * @param line - Input chat line
   * @return collection of sentence annotations
   */
  def getSentences(line: String) =
    JavaConversions.asScalaBuffer(getAnnotation(line)
      .get(classOf[CoreAnnotations.SentencesAnnotation])).toList

  /**
   * @param sentence - Input sentence
   * @return the sentence sentiment score
   */
  def getSentiment(sentence: CoreMap) = RNNCoreAnnotations.getPredictedClass(getTree(sentence))
}
