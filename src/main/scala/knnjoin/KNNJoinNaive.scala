package knnjoin

import com.vividsolutions.jts.geom.Point
import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._

object KNNJoinNaive extends KNNJoinSolver {
  override def solve(
                      dataRDD: PointRDD,
                      queryRDD: PointRDD,
                      k: Int
                    )
  : JavaPairRDD[Point, Point] = {
    dataRDD.spatialPartitioning(GridType.QUADTREE)
    dataRDD.buildIndex(IndexType.RTREE, false)
    dataRDD.buildIndex(IndexType.RTREE, true)

    val res = queryRDD.rawSpatialRDD.rdd.collect().flatMap(point => {
      KNNQuery.SpatialKnnQuery(dataRDD, point, k, true).toList.map((point, _))
    })
    JavaPairRDD.fromRDD(dataRDD.rawSpatialRDD.sparkContext.parallelize(res)).cache()
  }

}
