package knnjoin

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.datasyslab.geospark.spatialRDD.SpatialRDD

trait KNNJoinSolver extends Serializable {
  @throws(classOf[Exception])
  def solve(geometryFactory: GeometryFactory,
            dataRDD: SpatialRDD[Point],
            queryRDD: SpatialRDD[Point],
            k: Int,
            resultStr:StringBuilder, visualize: Boolean,
            outputPath: String)
  : JavaPairRDD[Point, java.util.List[Point]]

}
