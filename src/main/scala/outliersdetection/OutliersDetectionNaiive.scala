package outliersdetection

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import knnjoin.KNNJoinWithCirclesWithReduceByKey
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionNaiive {
  def findOutliersNaive(rdd: PointRDD, n: Int, k: Int): List[Point] = {
    rdd.buildIndex(IndexType.RTREE, false)
    rdd.indexedRDD.cache()

    val data = rdd.spatialPartitionedRDD.rdd.cache()
    println("Executing naiive outliers detection")

    var outliersCount = n
    val ans1 = data.collect().map(point => {
      (point, KNNQuery.SpatialKnnQuery(rdd, point, k, true).map(p2 => point.distance(p2)).max)
    }).groupBy(_._2).toList.sortBy(-_._1).takeWhile({ case (_, points) =>
      if (outliersCount > 0) {
        outliersCount -= points.length
        true
      } else {
        false
      }
    }).flatMap(_._2)


    val resRDD = new KNNJoinWithCirclesWithReduceByKey().solve(new GeometryFactory(), rdd, rdd, k, null, false, "")


    val ans2 = resRDD.rdd
      .map({ case (p, knn) => (p, knn.map(p.distance).max) })
      .takeOrdered(n)(Ordering.by[(Point, Double), Double](_._2).reverse)
      .toList


    outliersCount = n
    val ans3 = resRDD.rdd.map({ case (p, knn) => (p, knn.map(p.distance).max) })
      .collect()
      .groupBy(_._2)
      .toList.sortBy(-_._1).takeWhile({ case (_, points) =>
      if (outliersCount > 0) {
        outliersCount -= points.length
        true
      } else {
        false
      }
    }).flatMap(_._2)

    try {
      assert(ans3.containsAll(ans1) && ans3.containsAll(ans2))
    } catch {
      case e: AssertionError =>
        e.printStackTrace()
        println(s"[KNN native              = ${ans1.mkString(", ")}]")
        println(s"[KNNJoin                 = ${ans2.mkString(", ")}]")
        println(s"[KNNJoinMultipleOutliers = ${ans3.mkString(", ")}]")
        return ans1.map(_._1)
    }

    ans3.map(_._1)
  }
}
