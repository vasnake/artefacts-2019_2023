/** Created by vasnake@gmail.com on 2024-07-12
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`etl-core`._

case class ScoreAudienceModel(audienceName: String, predictor: Predictor) extends ComplexMLModel { // TODO: rename to ScoreAudience
  override def isOK: Boolean = true

  val groupedFeatures: GroupedFeatures = {
    val topics = (1 to 200).map(cnt => s"topic_$cnt")
    GroupedFeatures(groups =
      Seq(
        FeaturesGroup("all_profiles", Array(0, 1, 2, 2).map(_.toString)),
        FeaturesGroup("topics_m", topics.toArray)
      )
    )
  }

  private val TOPIC_THRESHOLD = 0.1

  override def apply(features: Array[Double]): Seq[Any] = {
    // features: (age, sex, joined, joined) default values: (0.5f, 0.5f, 0f, 0f)
    val mainFeatures: Array[Double] = features.take(4).zip(Array(0.5f, 0.5f, 0f, 0f)) map {
      case (value, defaultValue) =>
        if (value.isNaN) defaultValue
        else value
    }

    // topic_1 .. topic_200: (index, value) for each topic that not null and greater than threshold
    val topicFeatures: Array[(Int, Double)] = features
      .drop(4)
      .zipWithIndex
      .filter {
        case (value, _) =>
          !value.isNaN && value > TOPIC_THRESHOLD
      }
      .map(p => (p._2, p._1.toDouble))

    val score = Predictor.getScore(mainFeatures, topicFeatures, predictor)

    Seq(
      score, // score
      null, // scores_raw
      null, // scores_trf
      audienceName, // audience_name
      "positive" // category
    )
  }
}

object Predictor {

  /** Combine all-features array, compute dot product with predictor.W, return a final score
    * @param features part of all-features vector
    * @param topics part of all-features vector
    * @param predictor weights
    * @return predicted score
    */
  def getScore(
    features: Array[Double],
    topics: Array[(Int, Double)],
    predictor: Predictor
  ): Double = {
    // if topics.notEmpty: compute tf; extend main features
    val allFeatures: Array[(Int, Double)] = combineFeatures(features, topics, predictor)
    // compute dot product with weights
    val values: Array[Double] = allFeatures.map(_._2)
    val weights: Array[Double] = allFeatures.map(_._1).map(predictor.W)

    // Z = np.dot(W[fi], np.float64(fv)) + b
    val z = {
      // import org.nd4j.linalg.factory.Nd4j
      // import org.nd4s.Implicits._
      // val vecW = Nd4j.create(weights)
      // val vecV = Nd4j.create(values)
      // vecW.dot(vecV.T).sumNumber().doubleValue() + predictor.b
      val product = weights.zip(values).map { case (w, v) => w * v }
      product.sum + predictor.b
    }

    // SIGMOID: return (1./(1 + np.exp(Z)))
    1.0 / (math.exp(z) + 1.0)
  }

  /** If topics.notEmpty: compute tf using predictor.d_idf and extend main features
    * @param features part of all-features vector
    * @param topics part of all-features vector
    * @param predictor weights
    * @return all features with index
    */
  private def combineFeatures(
    features: Array[Double],
    topics: Array[(Int, Double)],
    predictor: Predictor
  ): Array[(Int, Double)] = {
    // tf = (1 + np.log(dv)) * d_idf[di]
    val tf1 = topics.map {
      case (idx, value) =>
        (idx, predictor.d_idf(idx) * (1.0 + math.log(value)))
    }

    val squaredSum = math.sqrt(
      tf1.map { case (_, value) => value * value }.sum
    )

    // topic features index starts from len(mainFeatures)
    val startIdx = features.length

    // tf /= np.sqrt(np.sum(tf**2))
    val topicFeatures = tf1.map {
      case (idx, value) =>
        (idx + startIdx, value / squaredSum)
    }

    val mainFeatures: Array[(Int, Double)] = features.zipWithIndex.map { case (v, i) => (i, v) }

    mainFeatures ++ topicFeatures
  }
}

case class Predictor(
  W: Array[Double],
  b: Double,
  d_idf: Array[Double],
  t_idf: Any = None,
  g_idf: Any = None
) {
  override def toString: String =
    s"Predictor(W=Array(${W.mkString(",")}), b=${b}, d_idf=Array(${d_idf.mkString(",")}), t_idf=${t_idf}, g_idf=${g_idf})"
}
