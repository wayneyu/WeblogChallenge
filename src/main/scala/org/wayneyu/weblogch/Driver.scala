package org.wayneyu.weblogch

import java.io.{File, PrintWriter}

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

    val colToKeep = Array(0, 2, 11) // val headers = Array("timestamp", "ip", "request")
    val sessionDurationMs = 5*60*1000

    val fileRDD = sc.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log")
    val csvRDD = fileRDD.map( _.split("\"\\s|\\s\"")
                        .zipWithIndex.flatMap{ case (v, ind) => if (ind != 1) v.split(" ") else Array(v) } )
    val indexedRDD = csvRDD.map( _.zipWithIndex.map(_.swap))
    val filteredRDD = indexedRDD.map( m => m.filter( kv => colToKeep.contains(kv._1)).map( _._2 ) )
    val keyedByIpRDD = filteredRDD.map( arr => (arr(1), List((arr(0),arr(2))))).reduceByKey( (a,b) => a:::b )
                                  .map{ case (ip, timeAndReqs) => (ip, timeAndReqs.sortBy( _._1 )) }
    keyedByIpRDD.cache()

    val sessionRDD = keyedByIpRDD.map{
      case (ip, timeAndReqs) => (ip, timeAndReqs.map{
          case (timestr, req) => (DateTime.parse(timestr).getMillis/sessionDurationMs, req)}.groupBy(_._1).map{
            case (session, sessionAndurls) => (session, sessionAndurls.map(_._2).toSet) }
          ) }

    val averageSessionMinsPerUserRDD = sessionRDD.map{ case (ip, sessionToUrls) =>
      (ip, avLengthConsecSeq(sessionToUrls.keys.toArray.sorted.toList)*sessionDurationMs/1000/60) }

    val averageSessionMins = averageSessionMinsPerUserRDD.map(_._2).mean().toInt

    sessionRDD.saveAsTextFile("data/res/sessionized_urls")
    averageSessionMinsPerUserRDD.saveAsTextFile("data/res/av_session_time")
    writeToFile("data/res/av_session_time/global_av", averageSessionMins.toString)
  }

  def diff(l: List[Long]): List[Long] = (0L::l).take(l.length).zip(l).map( p => p._2 - p._1)
  def average(l: List[Long]): Long = (l.sum/l.length.toDouble + 1).toLong
  def avLengthConsecSeq(l: List[Long]): Int = {
    val res = diff(0L::l).zipWithIndex.filter(_._1 != 1).map(_._2)
    average(diff(res.map(_.toLong))).toInt
  }

  def writeToFile(filename: String, content: String) = new PrintWriter(filename){ write(content); close() }


}