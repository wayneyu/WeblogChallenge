package org.wayneyu.weblogch

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.reflect.io.File

/**
  * Created by wayneyu on 5/26/16.
  */
object Driver {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("org.wayneyu.weblogch").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val colsToKeep = Array(0, 2, 11) // val headers = Array("timestamp", "ip", "request")
    val timeResolutionMs = 5*1000 //time resolution in ms
    val inActivityInMins = 30 //inactivity threshold in mins
    val inActivityThres = inActivityInMins*60*1000/timeResolutionMs //inactivity threshold in unit of time resolution

    // load file into memory
    val fileRDD = sc.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log")

    // Extraction begin ---------------------------------------------------------------------------------
    // Split each line by \"\s and \s\" first and then split by \s
    val csvRDD = fileRDD.map( _.split("\"\\s|\\s\"")
                        .zipWithIndex.flatMap{ case (v, ind) => if (ind != 1) v.split(" ") else Array(v) } )

    // Add column index to each value for each line, so that we can filter by column index later on
    val indexedRDD = csvRDD.map( _.zipWithIndex.map(_.swap))

    // Filter out irrelevant columns by keeping values with column index that we want to keep. Then remove the column indices
    // After this tranformation, the RDD is a collection of Array[String] where each array stores [timeString, ip, url]
    val filteredRDD = indexedRDD.map( m => m.filter( kv => colsToKeep.contains(kv._1)).map( _._2 ) )

    // Transformation begin ---------------------------------------------------------------------------------
    // Transform to a PairRDD datastructure, where each pair is (ip, (timeString, url))
    val keyedByIpRDD = filteredRDD.map( arr => (arr(1), List((arr(0),arr(2))))).reduceByKey( (a,b) => a:::b )

    // Convert datetime string to timestamp and discretize timestamp by timeResolution
    // After this transformation, the data structure will be a PairRDD where each row is
    // (ip, List of tuple of (time, Set of urls) )
    // or
    // (ip, [(time1, List(url1, url2)), (time1, List(url1, url2)),... ], ...)
    val ipToTimeToUrlRDD = keyedByIpRDD.map{
      case (ip, timeAndReqs) => (ip, timeAndReqs.map{
        case (timestr, req) => (DateTime.parse(timestr).getMillis/timeResolutionMs, req)})
    }


    // Analytics begin ---------------------------------------------------
    // Group unique urls within same timeResolution together
    val groupByTimeResRDD = ipToTimeToUrlRDD.map{
      case (ip, timeAndUrls) => (ip, timeAndUrls.groupBy(_._1).toArray.sortBy(_._1).map{
            case (session, sessionAndUrls) => (session, sessionAndUrls.map(_._2).toSet) }
          )}

    // Cache the final form of the data for following analytics
    groupByTimeResRDD.cache()

    // Sessionize by using an inactivity threshold (sessionTimeoutMs)
    val groupBySessionRDD = groupByTimeResRDD.map {
      case (ip, timeAndUrls) => (ip, sessionize(timeAndUrls.unzip._1.toList, inActivityThres).zip(timeAndUrls.unzip._2).groupBy(_._1))
    }

    val averageSessionSecsPerUserRDD = groupByTimeResRDD.map{ case (ip, timeToUrls) =>
      (ip, average(sessionLengths(timeToUrls.unzip._1.toList, inActivityThres))*timeResolutionMs/1000) }

    val averageSessionSecs = groupByTimeResRDD.flatMap{ case (ip, timeToUrls) =>
      sessionLengths(timeToUrls.unzip._1.toList, inActivityThres).filter(_!=0).map(_*timeResolutionMs/1000)}.mean()

    val longestSessionSecsPerUserRDD = groupByTimeResRDD.map{ case (ip, timeToUrls) =>
      (ip, sessionLengths(timeToUrls.unzip._1.toList, inActivityThres).max*timeResolutionMs/1000) }.sortBy(_._2, ascending = false)

    val resFolder = "data/res/"
    groupBySessionRDD.saveAsTextFile(resFolder + "sessionized_urls")
    averageSessionSecsPerUserRDD.saveAsTextFile(resFolder + "user_av_session_time")
    longestSessionSecsPerUserRDD.saveAsTextFile(resFolder + "user_max_session_time")
    writeToFile(resFolder + "av_session_time", averageSessionSecs.toString)
  }

  def diff(l: List[Long]): List[Long] = (l.head::l).zip(l).map(p => p._2 - p._1)

  def average(l: List[Long]): Long = l.sum/l.length
  def average(l: List[Int]): Int = l.sum/l.length

  def sessionize(ts: List[Long], thres: Long): List[Int] = {
    var session_id = 0
    for (t <- diff(ts)) yield {
      if (t > thres) session_id += 1
      session_id
    }
  }

  def sessionLengths(ts: List[Long], thres: Long): List[Int] =
    sessionize(ts, 10).zip(ts).groupBy(_._1).toList.map{ case (ind, sessionTs) => diff(sessionTs.unzip._2).sum.toInt }

  def avLengthConsecSeq(l: List[Long]): Int = {
    val res = diff(0L::l).zipWithIndex.filter(_._1 != 1).map(_._2)
    average(diff(res.map(_.toLong))).toInt
  }

  def writeToFile(filename: String, content: String) = new PrintWriter(filename){ write(content); close() }


}