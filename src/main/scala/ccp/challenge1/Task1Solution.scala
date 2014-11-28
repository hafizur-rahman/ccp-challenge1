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

    var seq: Seq[String] = Seq()

    val pageType = data.get("type").get
    val payload = data.getOrElse("payload", Map[String, Object]()).asInstanceOf[Map[String, Object]]

    pageType match {
      case "Account" =>
        if (payload.get("subAction").get.asInstanceOf[String] == "parentalControls") {
          seq = seq :+ "%s\tx:%s".format(key, payload.get("new").get)
        } else {
          seq = seq :+ "%s\tc:%s".format(key, payload.get("subAction").get)
        }
      case "AddToQueue" =>
        seq = seq :+ "%s\ta:%s".format(key, payload.getOrElse(itemIdKey,""))
      case "Home" =>
        seq = seq :+ "%s\tP:%s".format(key, payload.getOrElse("popular",""))
        seq = seq :+ "%s\tR:%s".format(key, payload.getOrElse("recommended",""))
        seq = seq :+ "%s\tr:%s".format(key, payload.getOrElse("recent",""))
      case "Hover" =>
        seq = seq :+ "%s\th:%s".format(key, payload.getOrElse(itemIdKey,""))
      case "ItemPage" =>
        seq = seq :+ "%s\ti:%s".format(key, payload.getOrElse(itemIdKey,""))
      case "Login" =>
        seq = seq :+ "%s\tL:".format(key)
      case "Logout" =>
        seq = seq :+ "%s\tl:".format(key)
      case "Play" | "Pause" | "Position" | "Stop" | "Advance" | "Resume" =>
        seq = seq :+ "%s\tp:%s,%s".format(key, payload.getOrElse("marker",""), payload.getOrElse(itemIdKey,""))
      case "Queue" =>
        seq = seq :+ "%s\tq:".format(key)
      case "Rate" =>
        seq = seq :+ "%s\tt:%s,%s".format(key, payload.getOrElse(itemIdKey,""), payload.getOrElse("rating", ""))
      case "Recommendations" =>
        seq = seq :+ "%s\tC:%s".format(key, payload.getOrElse("recs",""))
      case "Search" =>
        seq = seq :+ "%s\tS:%s".format(key, payload.getOrElse("results",""))
      case "VerifyPassword" =>
        seq = seq :+ "%s\tv:".format(key)
      case "WriteReview" =>
        seq = seq :+ "%s\tw:%s,%s,%s".format(key, payload.getOrElse(itemIdKey,""), payload.getOrElse("rating",""), payload.getOrElse("length",""))
    }
    seq
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

  def main(args: Array[String]) = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Demo")
    val sc = new SparkContext(conf)

    val path = args(0)
    val writeTo = args(1)
    val lines = sc.textFile(path)

    // 0. Explore data
    //    lines.flatMap(line => parseLine(line))
    //      .map(i => (i, 1))
    //      .reduceByKey(_ + _).sortByKey().foreach(println)

    // 1. Clean data
    lines.flatMap(cleanData).saveAsTextFile(writeTo)

    sc.stop()
  }
}