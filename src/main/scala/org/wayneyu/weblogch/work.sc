/**
  * Created by wayneyu on 5/26/16.
  */

//val t = List(2,3,4, 6,7, 10)
val l = List(123123333L)

def diff(l: List[Long]): List[Long] = (l.head::l).zip(l).map(p => p._2 - p._1)
def average(l: List[Long]): Long = (l.sum/l.length.toDouble + 1).toLong
def sessionize(ts: List[Long], thres: Long): List[Int] = {
  var session_id = 0
  for (t <- diff(ts)) yield {
    if (t > thres) session_id += 1
    session_id
  }
}

def sessionLengths(ts: List[Long], thres: Long): List[Int] = {
  sessionize(ts, 10).zip(ts).groupBy(_._1).toList.map{ case (ind, sessionTs) => diff(sessionTs.unzip._2).sum.toInt }
}

val ts = List(10).map(_.toLong)
sessionize(ts, 10)
ts.zip(sessionize(ts, 10))
ts.zip(sessionize(ts, 10)).groupBy(_._2).toArray
ts.zip(sessionize(ts, 10)).groupBy(_._2).toArray.map{ case (ind, sessionTs) => diff(sessionTs.unzip._1) }
sessionLengths(ts, 10)
sessionLengths(List(1,14), 10)
