package gnn

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import org.datasyslab.geospark.spatialRDD.PointRDD

object NaiveGNN extends GNNSolver {
  override def solve(geometryFactory: GeometryFactory, dataSpatialRDD: PointRDD,
                     querySpatialRDD: PointRDD, resultStr: StringBuilder, visualize: Boolean,
                     outputPath: String): (Map[String, String], (Point, Double))
  = {
    val dataRDD = dataSpatialRDD.rawSpatialRDD.rdd.cache()
    val queryRDD = querySpatialRDD.rawSpatialRDD.rdd.cache()

    val queryPoints = queryRDD.collect().toList
    val result = dataRDD
      .map(p => (
        p,
        queryPoints.map(q => q.distance(p)).sum
      ))
      .reduce((pa, pb) => {
        if (pa._2 <= pb._2)
          pa
        else
          pb
      })
    (Map(), result)
  }

}
