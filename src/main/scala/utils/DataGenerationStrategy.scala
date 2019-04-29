package utils

import java.util

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.SparkContext
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.util.Random

trait DataGenerationStrategy {
  def generate(size: Int, range: Int, sc: SparkContext): PointRDD
}

case class GenerateUniformData() extends DataGenerationStrategy {
  def generate(size: Int, range: Int, sc: SparkContext): PointRDD = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMin = Random.nextInt(2 * range / 3)
    val xBoundsMax = xBoundsMin + range
    val yBoundsMin = Random.nextInt(2 * range / 3)
    val yBoundsMax = yBoundsMin + range
    new PointRDD(
      sc.parallelize(
        for {
          _ <- 1 to size
        } yield
          geometryFactory.createPoint(
            new Coordinate(
              Random.nextDouble * (xBoundsMax - xBoundsMin) + xBoundsMin,
              Random.nextDouble * (yBoundsMax - yBoundsMin) + yBoundsMin
            )),
        16
      ))
  }
}

case class GenerateGuassianData() extends DataGenerationStrategy {
  def generate(size: Int, range: Int, sc: SparkContext): PointRDD = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMin = Random.nextInt(2 * range / 3)
    val xBoundsMax = xBoundsMin + range
    val yBoundsMin = Random.nextInt(2 * range / 3)
    val yBoundsMax = yBoundsMin + range
    new PointRDD(
      sc.parallelize(
        for {
          _ <- 1 to size
        } yield
          geometryFactory.createPoint(
            new Coordinate(
              (Random.nextGaussian + 4.0) / 8.0 * (xBoundsMax - xBoundsMin) + xBoundsMin,
              (Random.nextGaussian + 4.0) / 8.0 * (yBoundsMax - yBoundsMin) + yBoundsMin
            )),
        16
      ))
  }
}

case class GenerateExponentialData() extends DataGenerationStrategy {
  def generate(size: Int, range: Int, sc: SparkContext): PointRDD = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMin = Random.nextInt(2 * range / 3)
    val xBoundsMax = xBoundsMin + range
    val yBoundsMin = Random.nextInt(2 * range / 3)
    val yBoundsMax = yBoundsMin + range
    val lambda = 10
    new PointRDD(
      sc.parallelize(
        for {
          _ <- 1 to size
        } yield
          geometryFactory.createPoint(new Coordinate(
            math
              .log(1 - Random.nextDouble()) / -lambda * (xBoundsMax - xBoundsMin) + xBoundsMin,
            math
              .log(1 - Random.nextDouble()) / -lambda * (yBoundsMax - yBoundsMin) + yBoundsMin
          )),
        16
      ))
  }
}

case class GenerateNonUniformData() extends DataGenerationStrategy {
  def generate(size: Int, range: Int, sc: SparkContext): PointRDD = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMin: Double = Random.nextInt(2 * range / 3)
    val yBoundsMin: Double = Random.nextInt(2 * range / 3)

    val regionsCntPerAxis = 8
    var remainingPoints = size
    val regionIndices =
      Random.shuffle(for {
        i <- 0 until regionsCntPerAxis
        j <- 0 until regionsCntPerAxis
      } yield (i, j))

    val regions =
      for {
        (i, j) <- regionIndices
      } yield {
        val chosenCount =
          if (i + 1 < regionsCntPerAxis || j + 1 < regionsCntPerAxis) {
            Random.nextInt(
              math.min(size / regionsCntPerAxis / 2,
                       1 +
                         remainingPoints))
          } else {
            remainingPoints
          }
        remainingPoints -= chosenCount
        (xBoundsMin + range * (i + 1) / regionsCntPerAxis,
         yBoundsMin + range * (j + 1) / regionsCntPerAxis,
         chosenCount)
      }

    new PointRDD(
      sc.parallelize(
        regions.flatMap {
          case (xMinBounds, yMinBounds, count) => {
            for {
              _ <- 1 to count
            } yield
              geometryFactory.createPoint(
                new Coordinate(
                  Random.nextDouble * range / regionsCntPerAxis +
                    xMinBounds,
                  Random.nextDouble * range / regionsCntPerAxis +
                    yMinBounds
                ))
          }
        },
        16
      ))
  }
}

case class GenerateZipfData(skew: Double) extends DataGenerationStrategy {
  private def computeMap(size: Int, skew: Double) = {
    val map = new util.TreeMap[Double, Integer]
    var div = 0.0
    var i = 1
    while (i <= size) {
      div += 1.0 / math.pow(i, skew)

      {
        i += 1; i - 1
      }
    }
    var sum = 0.0
    i = 1
    while ({
      i <= size
    }) {
      val p = (1.0d / math.pow(i, skew)) / div
      sum += p
      map.put(sum, i - 1)

      {
        i += 1; i - 1
      }
    }
    map
  }

  def generate(size: Int, range: Int, sc: SparkContext): PointRDD = {
    val map = computeMap(size, skew)
    val geometryFactory = new GeometryFactory()
    val xBoundsMin = Random.nextInt(2 * range / 3)
    val xBoundsMax = xBoundsMin + range
    val yBoundsMin = Random.nextInt(2 * range / 3)
    val yBoundsMax = yBoundsMin + range

    def next: Int = {
      val value = Random.nextDouble
      map.ceilingEntry(value).getValue + 1
    }

    new PointRDD(
      sc.parallelize(
        (0 until size).map(
          _ =>
            geometryFactory.createPoint(
              new Coordinate(
                next * 1.0 / size * (xBoundsMax - xBoundsMin) + xBoundsMin,
                next * 1.0 / size * (yBoundsMax - yBoundsMin) + yBoundsMin
              )))

      ))
  }
}
