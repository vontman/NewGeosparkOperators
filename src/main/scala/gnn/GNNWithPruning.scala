package gnn

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import utils.{IndexNode, Visualization}

import scala.collection.mutable
import scala.util.Random

object GNNWithPruning {

  def apply(gridType: GridType, indexType: IndexType): GNNWithPruning = {
    if (indexType != IndexType.QUADTREE && indexType != IndexType.RTREE) {
      throw new RuntimeException("Only Quadtree and Rtree index are supported.")
    }
    new GNNWithPruning(gridType, indexType)
  }
}

class GNNWithPruning(gridType: GridType, indexType: IndexType)
    extends GNNSolver {
  def pruneOneLevel(dataRDD: RDD[Point],
                    queryRDD: RDD[IndexNode],
                    topResult: Double): (RDD[Point], RDD[IndexNode]) = {

    val queryBounds = queryRDD
      .map(
        node => (node.getBounds, node.getPointsCount)
      )
      .filter(_._1 != null)
      .collect()

    val filteredData = dataRDD
      .filter(
        p =>
          calculateEstimateTotalSum(
            p,
            queryBounds
          ) <= topResult
      )
      .cache()

    val nextLevelQueryRDD = queryRDD
      .flatMap(node => {
        val children = node.getChildren
        if (children.isEmpty) {
          List(node)
        } else {
          children
        }
      })

    (filteredData, nextLevelQueryRDD)
  }

  def calculateEstimateTotalSum(
      p: Point,
      queryBounds: Iterable[(Envelope, Int)]): Double = {
    queryBounds.map {
      case (mbr, count) => {
        mbr.distance(p.getEnvelopeInternal) * count
      }
    }.sum
  }

  def calculateRealTotalSum(p: Point, queryRDD: RDD[Point]): Double = {
    queryRDD
      .map(_.distance(p))
      .sum()
  }

  def solveNaive(dataPoints: Iterable[Point],
                 queryRDD: RDD[Point]): (Point, Double) = {

    val dataPointsWithId = dataPoints.map((Random.nextLong(), _))

    queryRDD
      .mapPartitions(queryPointsIter => {
        val queryPoints = queryPointsIter.toList
        dataPointsWithId.map {
          case (id, dataPoint) =>
            (id, (dataPoint, queryPoints.map(_.distance(dataPoint)).sum))
        }.toIterator
      })
      .reduceByKey((a, b) => (a._1, a._2 + b._2))
      .values
      .min()(Ordering.by(_._2))
    //    queryRDD
    //      .flatMap(queryPoint =>
    //        dataPointsWithId.map {
    //          case (id, dataPoint) =>
    //            (id, (dataPoint, queryPoint.distance(dataPoint)))
    //        }
    //      ).reduceByKey((a, b) => (a._1, a._2 + b._2)).values.min()(Ordering.by(_._2))
  }

  def getApproximateSolution(dataRDD: RDD[Point],
                             queryRDD: RDD[IndexNode]): Point = {
    val geometryFactory = new GeometryFactory()
    val min_points = queryRDD
      .map(
        node =>
          (
            geometryFactory.createPoint(
              new Coordinate(node.getAverageX, node.getAverageY)
            ),
            node.getPointsCount
        ))
      .collect()

    val topResult = dataRDD
      .map(
        p =>
          (
            p,
            min_points.map(x => p.distance(x._1) * x._2).sum
        ))
      .min()(Ordering.by(_._2))

    topResult._1

  }

  override def solve(
      geometryFactory: GeometryFactory,
      dataSpatialRdd: PointRDD,
      querySpatialRdd: PointRDD,
      resultStr: StringBuilder,
      visualize: Boolean,
      outputPath: String): (Map[String, String], (Point, Double)) = {

    val sparkContext = dataSpatialRdd.rawSpatialRDD.sparkContext
    var logs = mutable.HashMap.empty[String, String]

    {
      val t0 = System.currentTimeMillis()

      querySpatialRdd.spatialPartitioning(gridType)
      querySpatialRdd.buildIndex(indexType, true)

      logs += "setup" -> (System.currentTimeMillis() - t0).toString

    }

    val queryRDD = querySpatialRdd.indexedRDD.cache().rdd
    val queryPointsRDD = querySpatialRdd.spatialPartitionedRDD.cache().rdd
    val dataRDD = dataSpatialRdd.rawSpatialRDD.cache().rdd

    val MAX_LEVELS = queryRDD
      .map({
        case qtree: Quadtree => qtree.depth()
        case rtree: STRtree  => rtree.depth()
      })
      .max()

    val dataSize = dataRDD.count()
    val dataBounds = {
      val tmp = dataSpatialRdd.boundaryEnvelope
      tmp.expandToInclude(querySpatialRdd.boundaryEnvelope)
      tmp
    }

    var currDataRDD = dataRDD

    var currQueryRDD = queryRDD
      .map({
        case qtree: Quadtree => IndexNode(qtree.getRoot)
        case rtree: STRtree  => IndexNode(rtree.getRoot)
      })
      .cache()

    if (visualize) {
      Visualization.buildScatterPlot(List(querySpatialRdd, dataSpatialRdd),
                                     outputPath + "_level_0",
                                     dataBounds)
    }

    var level = 0
    val queryPointsCount = querySpatialRdd.countWithoutDuplicates()
    var usedQueryBounds = 0L
    while (level < MAX_LEVELS && (level < 5 || currQueryRDD
             .count() != usedQueryBounds) && currQueryRDD.count() < queryPointsCount / 10) {
      level += 1
      val t0 = System.currentTimeMillis()

      val currApproximationPoint =
        getApproximateSolution(currDataRDD, currQueryRDD)
      val currApproximationSum =
        calculateRealTotalSum(currApproximationPoint, queryPointsRDD)

      val (filteredDataRDD, nextLevelQueryRDD) =
        pruneOneLevel(currDataRDD, currQueryRDD, currApproximationSum)

      val dataCountBeforeFilter = currDataRDD.count()
      usedQueryBounds = currQueryRDD.count()
      val dataCountAfterFilter = filteredDataRDD.count()

      {
        resultStr.append("PRUNING STAGE " + level + "\n")
        resultStr.append("USING QUERY BOUNDS: " + usedQueryBounds + "\n")
        resultStr.append(
          "BEFORE: " + dataCountBeforeFilter + ", AFTER: " + dataCountAfterFilter + "\n")
        resultStr.append(
          "PRUNED: " + (dataCountBeforeFilter - dataCountAfterFilter) + "\n")
        resultStr.append(
          "TIME: " + (System.currentTimeMillis() - t0) + "millis\n")
        resultStr.append(
          "TOTAL PRUNING SO FAR: " + (dataSize - dataCountAfterFilter) + " = " + ((dataSize - dataCountAfterFilter) * 100.0 / dataSize) + "%" + "\n")
      }

      {
        print("USING QUERY BOUNDS: " + usedQueryBounds + "\n")
        print(
          "TOTAL PRUNING SO FAR: " + (dataSize - dataCountAfterFilter) + " = " + ((dataSize - dataCountAfterFilter) * 100.0 / dataSize) + "%" + "\n")
        print("TIME: " + (System.currentTimeMillis() - t0) + "millis\n")
      }

      {
        logs += "level_" + level + "_time" -> (System
          .currentTimeMillis() - t0).toString
        logs += "level_" + level + "_querybounds" -> usedQueryBounds.toString
        logs += "level_" + level + "_pruning" -> ((dataSize - dataCountAfterFilter) * 100.0 / dataSize).toString
        logs += "after_pruning" -> dataCountAfterFilter.toString
        logs += "pruning_percentage" -> ((dataSize - dataCountAfterFilter) * 100.0 / dataSize).toString
      }

      if (visualize) {
        val bounds = new PolygonRDD(
          currQueryRDD
            .map(_.getBounds)
            .filter(_ != null)
            .map(env => {
              geometryFactory.createPolygon(
                Array(
                  new Coordinate(env.getMinX, env.getMinY),
                  new Coordinate(env.getMinX, env.getMaxY),
                  new Coordinate(env.getMaxX, env.getMaxY),
                  new Coordinate(env.getMaxX, env.getMinY),
                  new Coordinate(env.getMinX, env.getMinY)
                ))
            }))
        bounds.analyze()

        val dataRDD = new PointRDD(filteredDataRDD)
        dataRDD.analyze()

        Visualization.buildScatterPlotWithResult(sparkContext,
                                                 List(bounds, dataRDD),
                                                 currApproximationPoint,
                                                 dataBounds,
                                                 outputPath + "_level_" + level)
      }

      currDataRDD = filteredDataRDD
      currQueryRDD = nextLevelQueryRDD

    }

    {
      resultStr.append(
        "FINDING REAL SOLUTION ON THE REMAINING " + currDataRDD
          .count() + " POINTS\n")

      println(
        "FINDING REAL SOLUTION ON THE REMAINING " + currDataRDD
          .count() + " POINTS\n")

      val t0 = System.currentTimeMillis()
      val dataPointsAfterPruning = currDataRDD.collect().toList
      val result = solveNaive(dataPointsAfterPruning, queryPointsRDD)

      resultStr.append(
        "TIME: " + (System.currentTimeMillis() - t0) + "millis\n")
      println("TIME: " + (System.currentTimeMillis() - t0) + "millis\n")
      logs += ("finding_real_sol" -> (System.currentTimeMillis() - t0).toString)

      (logs.toMap, result)
    }
  }
}
