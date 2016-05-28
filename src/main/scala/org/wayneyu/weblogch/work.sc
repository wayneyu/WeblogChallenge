/**
  * Created by wayneyu on 5/26/16.
  */

//val t = List(2,3,4, 6,7, 10)
val l = List(123123333L)

def diff(l: List[Long]): List[Long] = (l.head::l).take(l.length).zip(l).map( p => p._2 - p._1)
def average(l: List[Long]): Long = (l.sum/l.length.toDouble + 1).toLong
def sessionize(times: List[Long], inActivityThres: Long): List[Int] = {
  var session_id = 0
  for (t <- diff(times.toList)) yield {
    if (t > inActivityThres) session_id += 1
    session_id
  }
}
def sessionLengths(ts: List[Long], thres: Long) = sessionize(ts, thres).groupBy(identity).map(_._2.length).toList


sessionize(List(10,11,12,24,25,100), 10)
sessionLengths(List(10,11,12,13,24,25,100), 10)
