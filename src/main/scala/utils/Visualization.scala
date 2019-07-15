package utils

import java.awt.Color
import java.awt.image.BufferedImage

import com.vividsolutions.jts.geom._
import org.apache.spark.SparkContext
import org.datasyslab.geospark.spatialRDD.{PolygonRDD, SpatialRDD}
import org.datasyslab.geosparkviz.core.{ImageGenerator, RasterOverlayOperator}
import org.datasyslab.geosparkviz.extension.visualizationEffect.ScatterPlot
import org.datasyslab.geosparkviz.utils.ImageType

object Visualization {
  
  /**
    * Builds the scatter plot.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def buildScatterPlotWithResult(sparkContext: SparkContext,
                                 dataRDDs: List[SpatialRDD[_ <: Geometry]],
                                 solPoint: Point,
                                 outputPath: String): Boolean = {
    val bounds = dataRDDs
      .map(_.boundaryEnvelope)
      .reduceLeft((env, env2) => {
        env.expandToInclude(env2)
        env
      })

    buildScatterPlotWithResult(sparkContext,
                               dataRDDs,
                               solPoint,
                               bounds,
                               outputPath)

  }
  def buildScatterPlotWithResult(sparkContext: SparkContext,
                                 dataRDDs: List[SpatialRDD[_ <: Geometry]],
                                 solPoint: Point,
                                 bounds: Envelope,
                                 outputPath: String): Boolean = {

    val geometryFactory = new GeometryFactory()

    val minRange = math.max((bounds.getMaxX - bounds.getMinX) / 100,
                            (bounds.getMaxY - bounds.getMinY) / 100)

    val solRect = geometryFactory.createPolygon(
      Array(
        new Coordinate(solPoint.getX - minRange, solPoint.getY - minRange),
        new Coordinate(solPoint.getX + minRange, solPoint.getY - minRange),
        new Coordinate(solPoint.getX + minRange, solPoint.getY + minRange),
        new Coordinate(solPoint.getX - minRange, solPoint.getY + minRange),
        new Coordinate(solPoint.getX - minRange, solPoint.getY - minRange)
      ))

    val solRDD = new PolygonRDD(sparkContext.parallelize(for {
      _ <- 1 to 10
    } yield solRect))

    solRDD.analyze()
    buildScatterPlot(dataRDDs :+ solRDD, outputPath, bounds)
  }

  def buildScatterPlot(dataRDDs: Iterable[SpatialRDD[_ <: Geometry]],
                       outputPath: String): Boolean = {

    val range = dataRDDs
      .map(_.boundaryEnvelope)
      .reduceLeft((env, env2) => {
        env.expandToInclude(env2)
        env
      })
    buildScatterPlot(dataRDDs, outputPath, range)
  }

  def buildScatterPlot(dataRDDs: Iterable[SpatialRDD[_ <: Geometry]],
                       outputPath: String,
                       boundary: Envelope): Boolean = {
    val RES_X = 400
    val RES_Y = 400
    val COLORS = List(
      (Color.CYAN, Color.BLUE),
      (Color.ORANGE, Color.RED),
      (Color.RED, Color.RED),
      (Color.GREEN, Color.GREEN),
      (Color.MAGENTA, Color.BLUE),
      (Color.YELLOW, Color.GREEN)
    )

    boundary.expandBy(10)

    val visualizationOperators = dataRDDs.zipWithIndex.map({
      case (rdd, i) => {
        val operator = new ScatterPlot(RES_X, RES_Y, boundary, false)
        val (color, colorChannel) = COLORS(i)
        operator.CustomizeColor(color.getRed,
                                color.getGreen,
                                color.getBlue,
                                255,
                                colorChannel,
                                false)
        operator.Visualize(rdd.rawSpatialRDD.sparkContext, rdd)

        operator
      }
    })

    val blackImage =
      new BufferedImage(RES_X, RES_Y, BufferedImage.TYPE_INT_ARGB)
    val g = blackImage.getGraphics
    g.setColor(Color.BLACK)
    g.fillRect(0, 0, blackImage.getWidth(), blackImage.getHeight())

    val rasterOverlayOperator = new RasterOverlayOperator(blackImage)
    visualizationOperators.foreach(operator =>
      rasterOverlayOperator.JoinImage(operator.rasterImage))

    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(
      rasterOverlayOperator.backRasterImage,
      outputPath,
      ImageType.PNG)

  }

  /*def buildScatterPlot(sparkContext: SparkContext,
                       geometryFactory: GeometryFactory,
                       pairRDD: RDD[(Point, Point)],
                       dataRDD: SpatialRDD[Point],
                       queryRDD: SpatialRDD[Point],
                       outputPath: String)
  : Boolean = {
    val lineRDD = new LineStringRDD(
      pairRDD.map {
        case (p1, p2) => geometryFactory.createLineString(Array
        (p1.getCoordinate, p2.getCoordinate))
      })

    //    val dataRDD = new PointRDD(
    //      pairRDD.map(_._1)
    //    )
    //
    //    val queryRDD = new PointRDD(
    //      pairRDD.map(_._2)
    //    )


    val dataBounds = dataRDD.boundaryEnvelope
    dataBounds.expandToInclude(queryRDD.boundaryEnvelope)
    dataBounds.expandBy(20)

    val boundsRDD = new PolygonRDD(
      dataRDD
        .indexedRDD
        .rdd
        .map(index => index.asInstanceOf[STRtree].getRoot.getBounds
          .asInstanceOf[Envelope])
        .map(
          env => geometryFactory.createPolygon(Array(
            new Coordinate(env.getMinX, env.getMinY),
            new Coordinate(env.getMinX, env.getMaxY),
            new Coordinate(env.getMaxX, env.getMaxY),
            new Coordinate(env.getMaxX, env.getMinY),
            new Coordinate(env.getMinX, env.getMinY)
          ))
        ))

    val resX = 500
    val resY = 500

    val visualizationOperator1 = new ScatterPlot(resX, resY, dataBounds, false)
    visualizationOperator1.CustomizeColor(255, 255, 255, 255, Color.RED, true)
    visualizationOperator1.Visualize(sparkContext, lineRDD)

    val visualizationOperator2 = new ScatterPlot(resX, resY, dataBounds, false)
    visualizationOperator2.CustomizeColor(255, 255, 255, 255, Color.BLUE, true)
    visualizationOperator2.Visualize(sparkContext, dataRDD)

    val visualizationOperator3 = new ScatterPlot(resX, resY, dataBounds, false)
    visualizationOperator3.CustomizeColor(255, 255, 255, 255, Color.GREEN,
      true)
    visualizationOperator3.Visualize(sparkContext, queryRDD)

    val visualizationOperator4 = new ScatterPlot(resX, resY, dataBounds, false)
    visualizationOperator4.CustomizeColor(255, 255, 255, 255, Color.RED,
      true)
    visualizationOperator4.Visualize(sparkContext, boundsRDD)

    val mergedImage = new BufferedImage(resX, resY, BufferedImage.TYPE_INT_ARGB)
    val g = mergedImage.getGraphics
    g.setColor(Color.BLACK)
    g.fillRect(0, 0, mergedImage.getWidth(), mergedImage.getHeight())

    g.drawImage(visualizationOperator4.rasterImage, 0, 0, null)
    g.drawImage(visualizationOperator1.rasterImage, 0, 0, null)
    g.drawImage(visualizationOperator2.rasterImage, 0, 0, null)
    g.drawImage(visualizationOperator3.rasterImage, 0, 0, null)

    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(
      mergedImage,
      outputPath,
      ImageType.PNG)
    true
  }
 */

}
