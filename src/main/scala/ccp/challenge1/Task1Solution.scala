package ccp.challenge1

import java.text.SimpleDateFormat

import com.lambdaworks.jacks.JacksMapper

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Task1Solution {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Demo")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)
    val lines = sc.textFile(input)

    lines.map({ line =>
      val json = JacksMapper.readValue[Map[String, Any]](line)
      var items = List[Object]()

      items :+= json.getOrElse("played", Map[String, Integer]())
        .asInstanceOf[Map[String, Integer]]
        .keys

      if (json.contains("rated")){
        val ratedItems = json.get("rated").get.asInstanceOf[List[String]]
        ratedItems.foreach({item =>
          val matches = """([^:]*):([^,]*)""".r.findAllMatchIn(item)

          matches.foreach{ m => items :+= m.group(1) }
        })
      }

      if (json.contains("reviewed")){
        val reviewedItems = json.get("reviewed").get.asInstanceOf[List[String]]

        reviewedItems.foreach({ item =>
          val matches = """([^:]*):([^:]*):([^,]*)?""".r.findAllMatchIn(item)

          matches.foreach{ m => items :+= m.group(1) }
        })
      }

      val kidInfo = json.getOrElse("kid", List[Any]()).asInstanceOf[List[Any]]
      def isKid = (kidInfo.isEmpty, kidInfo.contains("true"), kidInfo.contains("false")) match {
        case (true, _, _) => None
        case (false, true, _) => true
        case (false, false, true) => false
      }

      (json.get("user").get, json.get("start").get, json.getOrElse("end", None), isKid, items.distinct.mkString(","))
    }).foreach(println)

  }
}