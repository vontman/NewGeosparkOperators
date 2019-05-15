package outliersdetection

import com.vividsolutions.jts.geom.Point
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionNaiive {
  def findOutliersNaive(rdd: PointRDD, k: Int, n: Int): List[Point] = {
    rdd.buildIndex(IndexType.RTREE, false)
    rdd.indexedRDD.cache()

    val data = rdd.spatialPartitionedRDD.rdd.cache()
    println("Executing naiive outliers detection")

    data.collect().flatMap(point => {
      KNNQuery.SpatialKnnQuery(rdd, point, k, true).map(p2 => (point.distance(p2), point))
    }).sortBy(_._1).takeRight(n).map(x => x._2).toList

    //    val resRDD = new KNNJoinWithCirclesWithReduceByKey().solve(new GeometryFactory(), rdd, rdd, k, null, false, "")
    //    resRDD.rdd
    //      .map({case (p, knn) => (p, knn.map(p.distance).max)})
    //      .takeOrdered(n)(Ordering.by[(Point, Double), Double](_._2).reverse)
    //      .map(_._1)
    //      .toList
  }
}
