package utils

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.SparkContext
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.util.Random

object InputGenerator {

  def generateInputData(size: Int, range: Int, sc: SparkContext): PointRDD = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMin = 0
    val xBoundsMax = range
    val yBoundsMin = 0
    val yBoundsMax = range
    new PointRDD(sc.parallelize(
      for {
        _ <- 1 to size
      } yield geometryFactory.createPoint(
        new Coordinate(
          Random.nextDouble * (xBoundsMax - xBoundsMin) + xBoundsMin,
          Random.nextDouble * (yBoundsMax - yBoundsMin) + yBoundsMin
        )), 8
    ))
  }


}
