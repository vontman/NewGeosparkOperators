package gnn

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import org.datasyslab.geospark.spatialRDD.PointRDD

trait GNNSolver extends Serializable {
  def solve(geometryFactory: GeometryFactory, dataRDD: PointRDD,
            queryRDD: PointRDD,
            resultStr:StringBuilder, visualize: Boolean,
                     outputPath: String)
  : (Map[String, String], (Point, Double))

}
