package utils

import java.util

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.apache.spark.SparkContext
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.util.Random

trait DataGenerationStrategy {

  def generate(sc: SparkContext,
               size: Int,
               range: Int = 1000000,
               offset: Double = 0.0,
               numPartitions: Int = -1): PointRDD = {
    val slices = if (numPartitions == -1) {
      sc.defaultParallelism
    } else {
      numPartitions
    }
    new PointRDD(
      sc.parallelize(
        generate(sc, size, range, range * offset, range * offset),
        slices
      )
    )
  }

  def generate(sc: SparkContext,
               size: Int,
               range: Int,
               xBoundsMin: Double,
               xBoundsMax: Double): Seq[Point]
}

case class GenerateUniformData() extends DataGenerationStrategy {
  def generate(sc: SparkContext,
               size: Int,
               range: Int,
               xBoundsMin: Double,
               yBoundsMin: Double): Seq[Point] = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMax = xBoundsMin + range
    val yBoundsMax = yBoundsMin + range
    for {
      _ <- 1 to size
    } yield
      geometryFactory.createPoint(
        new Coordinate(
          Random.nextDouble * (xBoundsMax - xBoundsMin) + xBoundsMin,
          Random.nextDouble * (yBoundsMax - yBoundsMin) + yBoundsMin
        ))
  }
}

case class GenerateGaussianData() extends DataGenerationStrategy {
  def generate(sc: SparkContext,
               size: Int,
               range: Int,
               xBoundsMin: Double,
               yBoundsMin: Double): Seq[Point] = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMax = xBoundsMin + range
    val yBoundsMax = yBoundsMin + range
    for {
      _ <- 1 to size
    } yield
      geometryFactory.createPoint(
        new Coordinate(
          (Random.nextGaussian + 4.0) / 8.0 * (xBoundsMax - xBoundsMin) + xBoundsMin,
          (Random.nextGaussian + 4.0) / 8.0 * (yBoundsMax - yBoundsMin) + yBoundsMin
        ))
  }
}

case class GenerateExponentialData() extends DataGenerationStrategy {
  def generate(sc: SparkContext,
               size: Int,
               range: Int,
               xBoundsMin: Double,
               yBoundsMin: Double): Seq[Point] = {
    val geometryFactory = new GeometryFactory()
    val xBoundsMax = xBoundsMin + range
    val yBoundsMax = yBoundsMin + range
    val lambda = 10
    for {
      _ <- 1 to size
    } yield
      geometryFactory.createPoint(new Coordinate(
        math
          .log(1 - Random.nextDouble()) / -lambda * (xBoundsMax - xBoundsMin) + xBoundsMin,
        math
          .log(1 - Random.nextDouble()) / -lambda * (yBoundsMax - yBoundsMin) + yBoundsMin
      ))
  }
}

case class GenerateNonUniformData() extends DataGenerationStrategy {
  def generate(sc: SparkContext,
               size: Int,
               range: Int,
               xBoundsMin: Double,
               yBoundsMin: Double): Seq[Point] = {
    val geometryFactory = new GeometryFactory()

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
    }
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
        i += 1;
        i - 1
      }
    }
    var sum = 0.0
    i = 1
    while ( {
      i <= size
    }) {
      val p = (1.0d / math.pow(i, skew)) / div
      sum += p
      map.put(sum, i - 1)

      {
        i += 1;
        i - 1
      }
    }
    map
  }

  def generate(sc: SparkContext,
               size: Int,
               range: Int,
               xBoundsMin: Double,
               yBoundsMin: Double): Seq[Point] = {
    val map = computeMap(size, skew)
    val geometryFactory = new GeometryFactory()
    val xBoundsMax = xBoundsMin + range
    val yBoundsMax = yBoundsMin + range

    def next: Int = {
      val value = Random.nextDouble
      map.ceilingEntry(value).getValue + 1
    }

    (0 until size).map(
      _ =>
        geometryFactory.createPoint(
          new Coordinate(
            next * 1.0 / size * (xBoundsMax - xBoundsMin) + xBoundsMin,
            next * 1.0 / size * (yBoundsMax - yBoundsMin) + yBoundsMin
          )))

  }
}

case class GenerateRandomGaussianClusters(clustersCount: Int, minPerCluster: Int, maxPerCluster: Int) extends DataGenerationStrategy {
  def generate(sc: SparkContext,
               size: Int,
               range: Int,
               xBoundsMin: Double,
               yBoundsMin: Double): Seq[Point] = {

    val geometryFactory = new GeometryFactory()

    val clustersCores = for (_ <- 1 to clustersCount) yield {
      new Coordinate(Random.nextDouble() * range + range / clustersCount, Random.nextDouble() * range + range / clustersCount)
    }

    clustersCores.flatMap(core => {
      val pointsInCluster = minPerCluster + Random.nextInt(maxPerCluster - minPerCluster)
      //      val pointsInCluster = size / clustersCount
      for (_ <- 1 to pointsInCluster) yield {
        geometryFactory.createPoint(
          new Coordinate(
            core.x + (Random.nextGaussian() + 4) / 8.0 * range / 2.0,
            core.y + (Random.nextGaussian() + 4) / 8.0 * range / 2.0
          ))
      }
    })
  }
}
