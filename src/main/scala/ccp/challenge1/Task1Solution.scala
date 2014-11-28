package ccp.challenge1

import com.lambdaworks.jacks.JacksMapper

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object Task1Solution {

  def parseLine(line: String): Seq[String] = {
    val cleaned = line.replaceAll ("\"\"", "\"")
    val json = JacksMapper.readValue[Map[String, Object]] (cleaned)
    val pageType = json.get ("type").get
    var seq: Seq[String] = Seq()

    json.foreach ( {i =>
    val (k, v) = (i._1, i._2)

    v match {
      case v: Map[String, Object] =>
        v.foreach ( {c => seq = seq :+ pageType + ":" + k + ":" + c._1 })
      case _ => seq = seq :+ pageType + ":" + k
      }
    })
    seq
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Demo")
    val sc = new SparkContext(conf)

    val path = args(0)
    val lines = sc.textFile(path)
    lines.flatMap(line => parseLine(line))
      .map(i => (i, 1))
      .reduceByKey(_ + _).sortByKey().foreach(println)

    sc.stop()
  }
}