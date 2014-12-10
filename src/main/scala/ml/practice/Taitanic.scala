package ml.practice

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkContext, SparkConf}
import au.com.bytecode.opencsv.CSVReader
import scala.collection.JavaConverters._

import java.io._

//survived,pclass,name,sex,age,sibsp,parch,ticket,fare,cabin,embarked
case class Taitanic(
                     survived: String,
                     pClass: String,
                     name: String,
                     sex: String,
                     age: String,
                     familyMembers: String,
                     parch: String,
                     ticket: String,
                     fare: String,
                     cabin: String,
                     embarked: String
                     ) {
  val idMap = List("child", "male", "female")
  val hClass = idMap.indexOf(if (age.isEmpty || age.toDouble < 16) "child" else sex)

  def gen_features: LabeledPoint = {
    val values = Vectors.dense(
      pClass.toDouble,
      hClass.toDouble,
      if (age.isEmpty) 0 else age.toDouble,
      familyMembers.toDouble
    )
    new LabeledPoint(survived.toInt, values)
  }

}

object Taitanic {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Demo")
    val sc = new SparkContext(conf)

    val csvData = sc.textFile("data/taitanic.csv")
    val taitanicData = csvData.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readAll().asScala.toList.map( p =>
        Taitanic(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10))
      )
    }.map(list => list(0))

    val parsedData = taitanicData.map(rec => rec.gen_features)

    // Split data into training and test
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 13L)
    val training = splits(0)
    val test = splits(1)

    val model = NaiveBayes.train(training, lambda = 1.0)

    val labelsAndPreds_nb = test.map(p => (model.predict(p.features), p.label))

    println(1.0 * labelsAndPreds_nb.filter(x => x._1 == x._2).count() / test.count())

    // Build the Decision Tree model
    val numClasses = 2
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
