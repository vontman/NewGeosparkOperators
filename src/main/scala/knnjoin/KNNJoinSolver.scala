package knnjoin

import com.vividsolutions.jts.geom.Point
import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.geospark.spatialRDD.PointRDD

trait KNNJoinSolver {
  @throws(classOf[Exception])
  def solve(
             dataRDD: PointRDD,
             queryRDD: PointRDD,
             k: Int
           )
  : JavaPairRDD[Point, Point]

}
