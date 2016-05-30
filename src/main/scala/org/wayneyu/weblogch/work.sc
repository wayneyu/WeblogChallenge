/**
  * Created by wayneyu on 5/26/16.
  */

//val t = List(2,3,4, 6,7, 10)
val l = List(123123333L)

def diff(l: List[Long]): List[Long] = { if (l.length == 1) List(0) else l.take(l.length-1).zip(l.drop(1)).map(p => p._2 - p._1) }
def average(l: List[Long]): Long = (l.sum/l.length.toDouble + 1).toLong
def sessionize(ts: List[Long], thres: Long): List[Int] = {
  var session_id = 0
  for (t <- 0L::diff(ts)) yield {
    if (t > thres) session_id += 1
    session_id
  }
}

def sessionLengths(ts: List[Long], thres: Long) =
  sessionize(ts, 10).zip(ts).groupBy(_._1).toList.map{ case (ind, sessionTs) => diff(sessionTs.unzip._2).sum.toInt }


val ts = List(5,6,10, 25, 26, 100, 140,142, 146).map(_.toLong)
sessionize(ts, 10)
ts.zip(sessionize(ts, 10))
sessionLengths(ts, 10)
sessionLengths(List(1,14), 10)
//res2: List[List[Long]] = List(List(100), List(25, 26), List(140, 142, 146), List(5, 6, 10))
