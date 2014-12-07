package ccp.challenge1

import java.text.SimpleDateFormat

import com.lambdaworks.jacks.JacksMapper

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Cleanup {

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

    if (data.contains("payload")) {
      val payload = data.get("payload").get.asInstanceOf[Map[String, Object]]
      if (payload.contains("itemId"))
        itemIdKey = "itemId"
      else if (payload.contains("itemID"))
        itemIdKey = "itemID"
    }

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
          Some("%s\tp:%s,%s".format(key, payload.getOrElse(itemIdKey,""), payload.getOrElse("marker","")))
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

  def merge[K, V](maps: Seq[Map[K, V]])(f: (K, V, V) => V): Map[K, V] = {
    maps.foldLeft(Map.empty[K, V]) { case (merged, m) =>
      m.foldLeft(merged) { case (acc, (k, v)) =>
        acc.get(k) match {
          case Some(existing) => acc.updated(k, f(k, existing, v))
          case None => acc.updated(k, v)
        }
      }
    }
  }

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
    lines.flatMap(cleanData).map({ line =>
      val parts = line.split("\t")
      val keyParts = parts(0).split(",")

      val parseData = {
        val data = mutable.HashMap[String, Any]()
        val dataParts = parts(1).split(":")

        data.put("user", keyParts(0))
        data.put("start", keyParts(1).toLong)
        data.put("sessionId", keyParts(2))

        if (dataParts.length > 1) {
          val items = dataParts(1).split(",")
          dataParts(0) match {
            case "C" =>
              data.put("recommendations", items.toSeq)
            case "P" =>
              data.put("popular", items.toSeq)
            case "R" =>
              data.put("recommended", items.toSeq)
            case "S" =>
              data.put("searched", items.toSeq)
            case "p" =>
              val movies = mutable.HashMap[String, Integer]()
              movies.put(items(0), items(1).toInt)
              data.put("played", movies)
            case "a" =>
              data.put("queued", items.toSeq)
            case "c" =>
              data.put("actions", items.toSeq)
              data.put("kid", Array(false.toString))
            case "h" =>
              data.put("hover", items.toSeq)
            case "i" =>
              data.put("browsed", items.toSeq)
            case "r" =>
              data.put("recent", items.toSeq)
            case "t" =>
              data.put("rated", Array(items.mkString(":")))
            case "w" =>
              data.put("reviewed", Array(items.mkString(":")))
            case "x" =>
              val isKid: Boolean = dataParts(1).equalsIgnoreCase("kid")
              data.put("kid", Array(isKid.toString))

              if (isKid)
                data.put("end", keyParts(1).toLong)
            case _ =>
          }
        } else {
          dataParts(0) match {
            case "L" =>
              data.put("actions", Array("login"))
            case "l" =>
              data.put("actions", Array("logout"))
            case "q" =>
              data.put("actions", Array("reviewedQueue"))
            case "v" =>
              data.put("actions", Array("verifiedPassword"))
              data.put("kid", Array(false.toString))
            case _ =>
          }
        }
        JacksMapper.writeValueAsString(data)
      }

      (keyParts(2), parseData)
    }).reduceByKey( (v1, v2) => {
      val data1 = JacksMapper.readValue[Map[String, Any]](v1)
      val data2 = JacksMapper.readValue[Map[String, Any]](v2)

      val merged = merge(Seq(data1, data2)){ (k, v1, v2) =>
        k match {
          case "user" | "sessionId" =>
            v1
          case "start" =>
            Math.min(v1.asInstanceOf[Long], v2.asInstanceOf[Long])
          case "played" =>
            (v1, v2) match {
              case (m1: Map[String, Integer], m2: Map[String, Integer]) =>
                merge(Seq(m1, m2)) { (_, t1, t2) =>
                  Math.max(t1, t2)
                }
              case (t1: Integer, t2: Integer) =>
                Math.max(t1, t2)
            }
          case _ =>
            (v1, v2) match {
              case (m1: Map[String, Array[String]], m2: Map[String, Array[String]]) =>
                m1 ++ m2
              case (l1: List[String], l2: List[String]) =>
                l1 ++ l2
            }
        }
      }

      JacksMapper.writeValueAsString(merged)
    }).map({ i => i._2})
      .saveAsTextFile(workDir + "/cleaned")

    sc.stop()
  }
}
