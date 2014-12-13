package ml.practice

import org.apache.spark.{SparkContext, SparkConf}


object Sentiment {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Demo")
    val sc = new SparkContext(conf)

    val aifnnWords = sc.textFile("data/AFINN-111.txt")
    val sentiments = aifnnWords.map { line =>
      val parts = line.split("\t")

      (parts(0), parts(1).toInt)
    }

    val vNegTerms = sentiments.filter(item => item._2 == -5 || item._2 == -4)
    val negTerms = sentiments.filter(item => item._2 == -3 || item._2 == -2 || item._2 == -1) ::
      List(, "second-rate", "moronic", "third-rate", "flawed", "juvenile", "boring", "distasteful", "ordinary", "disgusting", "senseless", "static", "brutal", "confused", "disappointing", "bloody", "silly", "tired", "predictable", "stupid", "uninteresting", "trite", "uneven", "outdated", "dreadful", "bland")
    val posTerms = sentiments.filter(item => item._2 == 3 || item._2 == 2 || item._2 == 1) ::
      List("first-rate", "insightful", "clever", "charming", "comical", "charismatic", "enjoyable", "absorbing", "sensitive", "intriguing", "powerful", "pleasant", "surprising", "thought-provoking", "imaginative", "unpretentious")
    val vPosTerms = sentiments.filter(item => item._2 == 4 || item._2 == 5) ::
      List("uproarious", "riveting", "fascinating", "dazzling", "legendary")
  }
}
