package ml.practice

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.classification.SVMWithSGD

case class Iris(
  sepal_l: Double,
  sepal_w: Double,
  petal_l: Double,
  petal_w: Double,
  species: String
)

object Iris {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Demo")
    val sc = new SparkContext(conf)

    val csvData = sc.textFile("data/iris.csv")
    val irisData = csvData.filter(!_.isEmpty).map { row =>
      val parts = row.split(",")
      val Array(sl, sw, pl, pw) = parts.slice(0,4).map(_.toDouble)

      Iris(sl, sw, pl, pw, parts(4))
    }

    irisData.take(2).foreach(println)

    val class2id = irisData.map(_.species).distinct.collect
      .zipWithIndex.map{case (k,v)=>(k, v.toDouble)}.toMap
    val id2class = class2id.map(_.swap)

    val parsedData = irisData.map {
      i => LabeledPoint(class2id(i.species), Vectors.dense(i.petal_l, i.petal_w, i.sepal_l, i.sepal_w))
    }

    // Split data into training and test
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 13L)
    val training = splits(0)
    val test = splits(1)

    val model = NaiveBayes.train(training, lambda = 1.0)

    val labelsAndPreds_nb = test.map(p => (model.predict(p.features), p.label))

    println(1.0 * labelsAndPreds_nb.filter(x => x._1 == x._2).count() / test.count())

    // Build the Decision Tree model
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 100
    val model_dt = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // Predict
    val labelsAndPreds_dt = test.map { point =>
      val pred = model_dt.predict(point.features)
      (pred, point.label)
    }

    println(1.0 * labelsAndPreds_dt.filter(x => x._1 == x._2).count() / test.count())

    sc.stop()
  }
}

