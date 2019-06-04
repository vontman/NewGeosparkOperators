package outliersdetection

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
//import knnjoin.KNNJoinWithCirclesWithReduceByKey
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
    data.collect().map(point => {
      (point, KNNQuery.SpatialKnnQuery(rdd, point, k, true).map(p2 => point.distance(p2)).max)
    }).groupBy(_._2).toList.sortBy(-_._1).takeWhile({ case (_, points) =>
      if (outliersCount > 0) {
        outliersCount -= points.length
        true
      } else {
        false
      }
    }).flatMap(_._2)
    .map(_._1)
  }
}
