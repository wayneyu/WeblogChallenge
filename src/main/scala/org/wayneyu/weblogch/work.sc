/**
  * Created by wayneyu on 5/26/16.
  */

//val t = List(2,3,4, 6,7, 10)
val l = List(123123333L)

def diff(l: List[Long]): List[Long] = (0L::l).take(l.length).zip(l).map( p => p._2 - p._1)
def average(l: List[Long]): Long = (l.sum/l.length.toDouble + 1).toLong
def avLengthConsecSeq(l: List[Long]): Int = {
  val res = diff(0L::l).zipWithIndex.filter(_._1 != 1).map(_._2)
  average(diff(res.map(_.toLong))).toInt
}

val res = diff(0L::l).zipWithIndex.filter(_._1 != 1).map(_._2)

avLengthConsecSeq(List(123123333))

