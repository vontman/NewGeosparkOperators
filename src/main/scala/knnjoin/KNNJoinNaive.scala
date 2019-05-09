package knnjoin

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import scala.collection.JavaConversions._

object KNNJoinNaive extends KNNJoinSolver {
  override def solve(geometryFactory: GeometryFactory,
                     dataRDD: SpatialRDD[Point],
                     queryRDD: SpatialRDD[Point],
                     k: Int,
                     resultStr: StringBuilder, visualize: Boolean,
                     outputPath: String)
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
