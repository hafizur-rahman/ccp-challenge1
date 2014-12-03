package ccp.challenge1

import java.text.SimpleDateFormat

import com.lambdaworks.jacks.JacksMapper

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object Task1Solution {

  def cleanData(line: String) = {
    val json = line.replaceAll("\"\"", "\"")
    val data = JacksMapper.readValue[Map[String, Object]] (json)

    var itemIdKey = "item_id"
    var sessionIdKey = "session_id"
    var createdAtKey = "created_at"

    if (data.contains("sessionId"))
      sessionIdKey = "sessionId"
    else if (data.contains("sessionID"))
      sessionIdKey = "sessionID"

    if (data.contains("createdAt"))
      createdAtKey = "createdAt"
    else if (data.contains("craetedAt"))
      createdAtKey = "createdAt"

    if (data.contains("payload") && data.get("payload").get.asInstanceOf[Map[String, Object]].contains("itemID"))
      itemIdKey = "itemId"

    val userId = data.get("user").get
    val sessionId = data.get(sessionIdKey).get

    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
    val timestamp = formatter.parse(data.get(createdAtKey).get.asInstanceOf[String])
    val key = "%s,%10d,%s".format(userId, timestamp.getTime, sessionId)

    val pageType = data.get("type").get
    val payload = data.getOrElse("payload", Map[String, Object]()).asInstanceOf[Map[String, Object]]

    pageType match {
      case "Account" =>
        if (payload.get("subAction").get.asInstanceOf[String] == "parentalControls") {
          Some("%s\tx:%s".format(key, payload.get("new").get))
        } else {
          Some("%s\tc:%s".format(key, payload.get("subAction").get))
        }
      case "AddToQueue" =>
        Some("%s\ta:%s".format(key, payload.getOrElse(itemIdKey,"")))
      case "Home" =>
        Some("%s\tP:%s".format(key, payload.getOrElse("popular","")))
        Some("%s\tR:%s".format(key, payload.getOrElse("recommended","")))
        Some("%s\tr:%s".format(key, payload.getOrElse("recent","")))
      case "Hover" =>
        Some("%s\th:%s".format(key, payload.getOrElse(itemIdKey,"")))
      case "ItemPage" =>
        Some("%s\ti:%s".format(key, payload.getOrElse(itemIdKey,"")))
      case "Login" =>
        Some("%s\tL:".format(key))
      case "Logout" =>
        Some("%s\tl:".format(key))
      case "Play" | "Pause" | "Position" | "Stop" | "Advance" | "Resume" =>
        if (payload.nonEmpty)
          Some("%s\tp:%s,%s".format(key, payload.getOrElse("marker",""), payload.getOrElse(itemIdKey,"")))
        else
          None
      case "Queue" =>
        Some("%s\tq:".format(key))
      case "Rate" =>
        Some("%s\tt:%s,%s".format(key, payload.getOrElse(itemIdKey,""), payload.getOrElse("rating", "")))
      case "Recommendations" =>
        Some("%s\tC:%s".format(key, payload.getOrElse("recs","")))
      case "Search" =>
        Some("%s\tS:%s".format(key, payload.getOrElse("results","").asInstanceOf[List[String]].mkString(",")))
      case "VerifyPassword" =>
        Some("%s\tv:".format(key))
      case "WriteReview" =>
        Some("%s\tw:%s,%s,%s".format(key, payload.getOrElse(itemIdKey,""), payload.getOrElse("rating",""), payload.getOrElse("length","")))
      case _ =>
        None
    }
  }

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

  case class Session(popular: Option[String] = None, recommended: Option[String] = None,
                     searched: Option[String] = None, hover: Option[String] = None,
                     queued: Option[String] = None, browsed: Option[String] = None,
                     recommendations: Option[String] = None, recent: Option[String] = None,
                     played: Option[String] = None, rated: Option[String] = None,
                     reviewed: Option[String] = None, actions: Option[String] = None,
                     kid: String = "kid", user: Option[String] = None,
                     start: Option[Long] = None, end: Option[Long] = None)

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Demo")
    val sc = new SparkContext(conf)

    val path = args(0)
    val workDir = args(1)
    val lines = sc.textFile(path)

    // 0. Explore data
    //    lines.flatMap(line => parseLine(line))
    //      .map(i => (i, 1))
    //      .reduceByKey(_ + _).sortByKey().foreach(println)

    // 1. Clean data
//    lines.flatMap(cleanData).saveAsTextFile(workDir + "/cleaned")

    val cleaned = sc.textFile(workDir + "/cleaned/part-00000")
      .map({ line =>
        val parts = line.split("\t")
        val keyParts = parts(0).split(",")

        (keyParts(0) + ":" + keyParts(2), (keyParts(1).toLong, keyParts(1).toLong, parts(1)))
      }).reduceByKey( (v1, v2) => {
        val min = Math.min(v1._1, v2._1)
        val max = Math.max(v1._2, v2._2)

        (min, max, v1._3 + ";" + v2._3)
      }).take(1000).foreach(println)
    sc.stop()
  }
}