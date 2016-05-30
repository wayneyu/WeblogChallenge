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

    val conf = new SparkConf().setAppName("org.wayneyu.weblogch").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val filename = "data/2015_07_22_mktplace_shop_web_log_sample.log"
    val resFolder = "data/res1s/"
    val timeResolutionMs = 1*1000L //time resolution in ms
    val inActivityInMins = 30 //inactivity threshold in mins
    val inActivityThres = inActivityInMins*60*1000/timeResolutionMs //inactivity threshold in unit of time resolution
    val colsToKeep = Array(0, 2, 11) // val headers = Array("timestamp", "ip", "request")

    // load file into memory
    val fileRDD = sc.textFile(filename)

    // Extraction begin ---------------------------------------------------------------------------------
    // Split each line by \"\s and \s\" first and then split by \s
    val csvRDD = fileRDD.map( _.split("\"\\s|\\s\"")
                        .zipWithIndex.flatMap{ case (v, ind) => if (ind != 1) v.split(" ") else Array(v) } )

    // Add column index to each value for each line, so that we can filter by column index later on
    val indexedRDD = csvRDD.map( _.zipWithIndex.map(_.swap))

    // Filter out irrelevant columns by keeping values with column index that we want to keep. Then remove the column indices
    // After this tranformation, the RDD is a collection of Array[String] where each array stores [timeString, ip, url]
    val filteredRDD = indexedRDD.map( e => e.filter( kv => colsToKeep.contains(kv._1)).map( _._2 ) )

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

    // Cache the data for the following analytics
    groupByTimeResRDD.cache()

    // Sessionize by inactivity threshold (sessionTimeoutMs)
    val groupBySessionRDD = groupByTimeResRDD.map {
      case (ip, timeAndUrls) => (ip, sessionize(timeAndUrls.unzip._1.toList, inActivityThres).zip(timeAndUrls.unzip._2).groupBy(_._1)
        .map{ case (sessionId, sessionIdAndUrls) => (sessionId, sessionIdAndUrls.flatMap(_._2).toSet) })
    }

    // Find session lengths for each user and cache the result for later session calculations
    val userSessionLengthSecs = groupByTimeResRDD.map{ case (ip, timeToUrls) =>
      (ip, sessionLengths(timeToUrls.unzip._1.toList, inActivityThres).map(_*timeResolutionMs/1000).map(_.toInt)) }
    userSessionLengthSecs.cache()
    groupByTimeResRDD.unpersist() // to free up memory

    // Find the average user session length
    // We skip the sessions with length 0, so that they don't skew the result
    val userAverageSessionSecsRDD = userSessionLengthSecs.map{ case (ip, sessionLengths) => (ip, average(sessionLengths.filter(_!=0))) }

    // Find the average session length
    val averageSessionSecs = userSessionLengthSecs.flatMap{ case (ip, sessionLengths) => sessionLengths.filter(_!=0) }.mean()

    // Find the longest user session length and sort the result by session length
    val userLongestSessionSecsRDD = userSessionLengthSecs.map{ case (ip, sessionLengths) => (ip, sessionLengths.max) }.sortBy(_._2, ascending = false)

    // Save results
    groupBySessionRDD.saveAsTextFile(resFolder + "sessionized_urls")
    userAverageSessionSecsRDD.saveAsTextFile(resFolder + "user_av_session_time")
    userLongestSessionSecsRDD.saveAsTextFile(resFolder + "user_max_session_time")
    writeToFile(resFolder + "av_session_time", averageSessionSecs.toString)
  }

  // Find difference between consecutive element of a list, ie.. diff(l)(i) = l(i) - l(i-1).
  // If the list has only one element, we return List(0).
  // This is because if there is only one log entry for a user, when calculating the time difference between user requests,
  // the session time is not defined and setting the time difference to 0 allow us to exclude those cases.
  def diff(l: List[Long]): List[Long] = { if (l.length == 1) List(0) else l.take(l.length-1).zip(l.drop(1)).map(p => p._2 - p._1) }

  def average(l: List[Int]): Int = { if (l.isEmpty) 0 else l.sum/l.length }

  // Label a list of times with a session number
  // A session starts when time elapsed since last time is longer than an inactivity period (thres)
  def sessionize(ts: List[Long], thres: Long): List[Int] = {
    var session_id = 0
    for (t <- 0L::diff(ts)) yield {
      if (t > thres) session_id += 1
      session_id
    }
  }

  // Find the session lengths for a list of times
  // First, each time is labeled with a session index.
  // Then, we find the difference between consecutive times for each session
  // Lastly, we sum up the time differences for each session
  def sessionLengths(ts: List[Long], thres: Long): List[Int] =
    sessionize(ts, thres).zip(ts).groupBy(_._1).toList.map{ case (ind, sessionTs) => diff(sessionTs.unzip._2).sum.toInt }

  def writeToFile(filename: String, content: String) = new PrintWriter(filename){ write(content); close() }
}