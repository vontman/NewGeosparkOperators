package knnjoin

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD

object KNNJoinNaive extends KNNJoinSolver {
  override def solve(geometryFactory: GeometryFactory,
                     dataRDD: SpatialRDD[Point],
                     queryRDD: SpatialRDD[Point],
                     k: Int,
                     resultStr: StringBuilder, visualize: Boolean,
                     outputPath: String)
  : JavaPairRDD[Point, java.util.List[Point]] = {
    dataRDD.buildIndex(IndexType.RTREE, false)

    val res = queryRDD.rawSpatialRDD.rdd.collect().map(point => {
      (point, KNNQuery.SpatialKnnQuery(dataRDD, point, k, true))
    })
    assert(res.size == queryRDD.countWithoutDuplicates())
    assert(res.forall(_._2.size() == k))

    JavaPairRDD.fromRDD(dataRDD.rawSpatialRDD.sparkContext.parallelize(res)).cache()
  }

}
