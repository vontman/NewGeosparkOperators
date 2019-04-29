package gnn

import com.vividsolutions.jts.geom.{
  Coordinate,
  Envelope,
  GeometryFactory,
  Point
}
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree.{Node, NodeBase, Quadtree}
import com.vividsolutions.jts.index.strtree.{AbstractNode, Boundable, STRtree}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import utils.Visualization
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

object GNNWithPruning extends GNNSolver {

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

    //    queryRDD
    //      .flatMap(queryPoint =>
    //        dataPointsWithId.map {
    //          case (id, dataPoint) =>
    //            (id, (dataPoint, queryPoint.distance(dataPoint)))
    //        }
    //      ).reduceByKey((a, b) => (a._1, a._2 + b._2)).min()(Ordering.by(_._2._2))._2
    //      ).aggregateByKey((new Point(), 0.0))((a, b) => (a._1, a._2 + b._2), (a, b) => (a._1, a._2 + b._2)).min()(Ordering.by(_._2._2))._2
    //    queryRDD
    //      .flatMap(queryPoint =>
    //        dataPointsWithId.map {
    //          case (id, dataPoint) =>
    //            (id, (dataPoint, queryPoint.distance(dataPoint)))
    //        }
    //      ).values.take(1)(0)

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

  def pruneOneLevelQuadTree(
      dataRDD: RDD[Point],
      queryRDD: RDD[NodeBase],
      topResult: Double,
      visualize: Boolean,
      outputPath: String,
      Q: RDD[SpatialIndex]): (RDD[Point], RDD[NodeBase]) = {

    val queryBounds = queryRDD
      .map(
        node => (node.getBounds, node.size)
      )
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
      .flatMap(
        node => {
          if (node.hasChildren && node.size() > 20) {
            val children = node.getSubnode
              .filter(node => node != null && !node.isEmpty)
              .toList

            if (node.getItems.size() != 0) {
              val tmp = new Node(node.getBounds, 1)
              tmp.addAllItems(node.getItems)
              tmp :: children
            } else {
              children
            }
          } else {
            List(node)
          }
        }
      )

    (filteredData, nextLevelQueryRDD)
  }

  def pruneOneLevelRTree(dataRDD: RDD[Point],
                         queryRDD: RDD[Boundable],
                         topResult: Double,
                         visualize: Boolean,
                         outputPath: String,
                         Q: RDD[SpatialIndex]): (RDD[Point], RDD[Boundable]) = {

    val queryBounds = queryRDD
      .map(
        node => (node.getBounds.asInstanceOf[Envelope], node.pointsCount())
      )
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
      .flatMap(
        node =>
          if (!node.isInstanceOf[AbstractNode]) {
            List(node)
          } else {
            val abstractNode = node
              .asInstanceOf[AbstractNode]

            if (abstractNode.getLevel == 0) {
              List(node)
            } else {
              abstractNode.getChildBoundables
                .map(node => node.asInstanceOf[Boundable])
            }
        }
      )
      .filter(_ != null)

    (filteredData, nextLevelQueryRDD)
  }

  def calculateRealTotalSum(p: Point, queryRDD: RDD[Point]): Double = {
    queryRDD
      .map(_.distance(p))
      .sum()
  }

  def getApproximateSolutionQuadTree(dataRDD: RDD[Point],
                                     queryRDD: RDD[SpatialIndex]): Point = {
    val geometryFactory = new GeometryFactory()
    val min_points = queryRDD
      .mapPartitions(spatialIndices => {
        var xSum = 0.0
        var ySum = 0.0
        var count = 0
        spatialIndices.foreach(index => {
          val bounds: Envelope = index
            .asInstanceOf[Quadtree]
            .getRoot
            .getBounds
          val points = index.query(bounds).toArray()
          points.foreach(p => {
            val q = p.asInstanceOf[Point]
            xSum += q.getX
            ySum += q.getY
            count += 1
          })
        })
        List(
          (geometryFactory.createPoint(
             new Coordinate(xSum / count, ySum / count)),
           count)
        ).iterator
      })
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

  def getApproximateSolutionRTree(dataRDD: RDD[Point],
                                  queryRDD: RDD[SpatialIndex]): Point = {
    val geometryFactory = new GeometryFactory()
    val min_points = queryRDD
      .mapPartitions(spatialIndices => {
        var xSum = 0.0
        var ySum = 0.0
        var count = 0
        spatialIndices.foreach(index => {
          val bounds: Envelope = index
            .asInstanceOf[STRtree]
            .getRoot
            .getBounds
            .asInstanceOf[Envelope]
          val points = index.query(bounds).toArray()
          points.foreach(p => {
            val q = p.asInstanceOf[Point]
            xSum += q.getX
            ySum += q.getY
            count += 1
          })
        })
        List(
          (geometryFactory.createPoint(
             new Coordinate(xSum / count, ySum / count)),
           count)
        ).iterator
      })
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

  def getApproximateSolutionQuadTreePerLevel(dataRDD: RDD[Point],
                                             queryRDD: RDD[NodeBase]): Point = {
    val geometryFactory = new GeometryFactory()
    val min_points = queryRDD
      .map(
        node =>
          (
            geometryFactory.createPoint(
              new Coordinate(node.averageX(), node.averageY())
            ),
            node.size
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
      //      querySpatialRdd.spatialPartitioning(GridType.RTREE)
      //      querySpatialRdd.buildIndex(IndexType.RTREE, true)
      querySpatialRdd.spatialPartitioning(GridType.QUADTREE)
      querySpatialRdd.buildIndex(IndexType.QUADTREE, true)

      logs += "setup" -> (System.currentTimeMillis() - t0).toString

    }

    val queryRDD = querySpatialRdd.indexedRDD.cache().rdd
    val queryPointsRDD = querySpatialRdd.spatialPartitionedRDD.cache().rdd
    val dataRDD = dataSpatialRdd.rawSpatialRDD.cache().rdd

    //    val MAX_LEVELS = 5
    val MAX_LEVELS = queryRDD.map(_.asInstanceOf[Quadtree].depth()).max()

    val dataSize = dataRDD.count()
    val dataBounds = {
      val tmp = dataSpatialRdd.boundaryEnvelope
      tmp.expandToInclude(querySpatialRdd.boundaryEnvelope)
      tmp
    }

//    val approximateSolution =
//      calculateRealTotalSum(getApproximateSolutionQuadTree(dataRDD, queryRDD),
//                            queryPointsRDD)

    //    val approximateSolution =
    //      calculateRealTotalSum(getApproximateSolutionRTree(dataRDD, queryRDD),
    //                            queryPointsRDD)

    var currDataRDD = dataRDD
    //    var currQueryRDD = queryRDD
    //      .map(index => index.asInstanceOf[STRtree].getRoot.asInstanceOf[Boundable])
    //      .filter(_ != null)
    //      .cache()

    var currQueryRDD = queryRDD
      .map(index => index.asInstanceOf[Quadtree].getRoot.asInstanceOf[NodeBase])
      .filter(_ != null)
      .cache()

    if (visualize) {
      Visualization.buildScatterPlot(List(querySpatialRdd, dataSpatialRdd),
                                     outputPath + "_level_0",
                                     dataBounds)
    }

    var level = 0
    val queryPointsCount = querySpatialRdd.countWithoutDuplicates()
    var usedQueryBounds = 0L
    while (level < MAX_LEVELS && currQueryRDD.count() < queryPointsCount / 10) {
      level += 1
      val t0 = System.currentTimeMillis()

      val currApproximation =
        getApproximateSolutionQuadTreePerLevel(currDataRDD, currQueryRDD)
      val currApproximationSum =
        calculateRealTotalSum(currApproximation, queryPointsRDD)

      val (filteredDataRDD, nextLevelQueryRDD) = pruneOneLevelQuadTree(
        currDataRDD,
        currQueryRDD,
        currApproximationSum,
        visualize,
        outputPath,
        queryRDD)

      val dataCountBeforeFilter = currDataRDD.count()
      usedQueryBounds = currQueryRDD.count()
      val dataCountAfterFilter = filteredDataRDD.count()

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

      //      print("PRUNING STAGE " + level + "\n")
      print("USING QUERY BOUNDS: " + usedQueryBounds + "\n")
      //      print(
      //        "BEFORE: " + dataCountBeforeFilter + ", AFTER: " + dataCountAfterFilter + "\n")
      //      print("PRUNED: " + (dataCountBeforeFilter - dataCountAfterFilter) + "\n")
      print(
        "TOTAL PRUNING SO FAR: " + (dataSize - dataCountAfterFilter) + " = " + ((dataSize - dataCountAfterFilter) * 100.0 / dataSize) + "%" + "\n")
      print("TIME: " + (System.currentTimeMillis() - t0) + "millis\n")
      logs += "level_" + level + "_time" -> (System
        .currentTimeMillis() - t0).toString
      logs += "level_" + level + "_pruning" -> ((dataSize - dataCountAfterFilter) * 100.0 / dataSize).toString
      logs += "after_pruning" -> dataCountAfterFilter.toString
      logs += "pruning_percentage" -> ((dataSize - dataCountAfterFilter) * 100.0 / dataSize).toString

      if (visualize) {
        val bounds = new PolygonRDD(
          currQueryRDD
            .map(_.getBounds.asInstanceOf[Envelope])
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
                                                 currApproximation,
                                                 dataBounds,
                                                 outputPath + "_level_" + level)
        //        Visualization.buildScatterPlot(List(dataRDD, bounds),
        //          outputPath + "_level_" + level, dataBounds)
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

  /*
  def visualizeBeforeAndAfterPruning(
      sparkContext: SparkContext,
      dataAfterRDDs: List[(PointRDD, PolygonRDD)],
      queryRDD: SpatialRDD[Point],
      bestSolPoint: Point,
      outputPath: String): Unit = {

    val dataBounds = {
      val bounds = queryRDD.boundaryEnvelope
      dataAfterRDDs.foreach({
        case (rdd, _) => bounds.expandToInclude(rdd.boundaryEnvelope)
      })
      bounds.expandBy(20)
      bounds
    }

    val solRect = {
      val geometryFactory = new GeometryFactory()

      val minRange = math.max((dataBounds.getMaxX - dataBounds.getMinX) / 100,
                              (dataBounds.getMaxY - dataBounds.getMinY) / 100)

      geometryFactory.createPolygon(
        Array(
          new Coordinate(bestSolPoint.getX - minRange,
                         bestSolPoint.getY - minRange),
          new Coordinate(bestSolPoint.getX + minRange,
                         bestSolPoint.getY - minRange),
          new Coordinate(bestSolPoint.getX + minRange,
                         bestSolPoint.getY + minRange),
          new Coordinate(bestSolPoint.getX - minRange,
                         bestSolPoint.getY + minRange),
          new Coordinate(bestSolPoint.getX - minRange,
                         bestSolPoint.getY - minRange)
        ))

    }

    val bestSolPointRDD = new PolygonRDD(sparkContext.parallelize(for {
      _ <- 1 to 10
    } yield solRect))

    val resX = 300
    val resY = 300

    val images = {

      val dataAfterVisualizationOperators = dataAfterRDDs.map({
        case (dataAfterRDD, queryBoundsRDD) => {
          val dataOperator = new ScatterPlot(resX, resY, dataBounds, false)
          dataOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
          dataOperator.Visualize(sparkContext, dataAfterRDD)

          val queryOperator = new ScatterPlot(resX, resY, dataBounds, false)
          queryOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
          queryOperator.Visualize(sparkContext, queryBoundsRDD)

          (dataOperator, queryOperator)
        }
      })

      val queryVisualizationOperator =
        new ScatterPlot(resX, resY, dataBounds, false)
      queryVisualizationOperator.CustomizeColor(255,
                                                255,
                                                255,
                                                255,
                                                Color.BLUE,
                                                true)
      queryVisualizationOperator.Visualize(sparkContext, queryRDD)

      //      val queryBoundsVisualizationOperator = new ScatterPlot(resX, resY, dataBounds, false)
      //      queryBoundsVisualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN,
      //        true)
      //      queryBoundsVisualizationOperator.Visualize(sparkContext, queryIndexRDD)

      val bestSolVisualizationOperator =
        new ScatterPlot(resX, resY, dataBounds, false)
      bestSolVisualizationOperator.CustomizeColor(255,
                                                  0,
                                                  255,
                                                  255,
                                                  Color.GREEN,
                                                  true)
      bestSolVisualizationOperator.Visualize(sparkContext, bestSolPointRDD)

      val afterImages =
        dataAfterVisualizationOperators.map({
          case (dataAfterVisualizationOperator,
                queryBoundsVisualizationOperator) => {
            val afterImage =
              new BufferedImage(resX, resY, BufferedImage.TYPE_INT_ARGB)
            val afterImageG = afterImage.getGraphics
            afterImageG.setColor(Color.BLACK)
            afterImageG.fillRect(0, 0, resX, resY)

            afterImageG.drawImage(queryVisualizationOperator.rasterImage,
                                  0,
                                  0,
                                  null)
            afterImageG.drawImage(queryBoundsVisualizationOperator.rasterImage,
                                  0,
                                  0,
                                  null)
            afterImageG.drawImage(bestSolVisualizationOperator.rasterImage,
                                  0,
                                  0,
                                  null)
            afterImageG.drawImage(dataAfterVisualizationOperator.rasterImage,
                                  0,
                                  0,
                                  null)
            afterImage
          }
        })

      afterImages
    }

    val imageGenerator = new ImageGenerator()

    for ((img, ind) <- images.view.zipWithIndex) {
      imageGenerator
        .SaveRasterImageAsLocalFile(img,
                                    outputPath + "after_" + ind,
                                    ImageType.PNG)
    }
  }

 */

}
