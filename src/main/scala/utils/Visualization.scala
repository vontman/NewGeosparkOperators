package utils

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.FileInputStream
import java.util.Properties

import com.vividsolutions.jts.geom.{
  Coordinate,
  Envelope,
  Geometry,
  GeometryFactory,
  Point
}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.spatialRDD.{PolygonRDD, SpatialRDD}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparkviz.core.{ImageGenerator, RasterOverlayOperator}
import org.datasyslab.geosparkviz.extension.visualizationEffect.ScatterPlot
import org.datasyslab.geosparkviz.utils.ImageType

object Visualization {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("GeoSparkVizDemo")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator",
           classOf[GeoSparkVizKryoRegistrator].getName)
      .setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val prop = new Properties()
    val resourcePath = "src/test/resources/"
    val demoOutputPath = "target/demo"
    var ConfFile = new FileInputStream(
      resourcePath + "babylon.point.properties")
    prop.load(ConfFile)
    val scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot"
    //  val heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap"
    //  val choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap"
    //  val parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrenderstitchheatmap"
    //  val earthdataScatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/earthdatascatterplot"
    //  val PointInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
    //  val PointOffset = prop.getProperty("offset").toInt
    //  val PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    //  val PointNumPartitions = prop.getProperty("numPartitions").toInt
    //  ConfFile = new FileInputStream(resourcePath + "babylon.rectangle.properties")
    //  prop.load(ConfFile)
    //  val RectangleInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
    //  val RectangleOffset = prop.getProperty("offset").toInt
    //  val RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    //  val RectangleNumPartitions = prop.getProperty("numPartitions").toInt
    //  ConfFile = new FileInputStream(resourcePath + "babylon.polygon.properties")
    //  prop.load(ConfFile)
    //  val PolygonInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
    //  val PolygonOffset = prop.getProperty("offset").toInt
    //  val PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    //  val PolygonNumPartitions = prop.getProperty("numPartitions").toInt
    //  ConfFile = new FileInputStream(resourcePath + "babylon.linestring.properties")
    //  prop.load(ConfFile)
    //  val LineStringInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
    //  val LineStringOffset = prop.getProperty("offset").toInt
    //  val LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
    //  val LineStringNumPartitions = prop.getProperty("numPartitions").toInt
    //  val USMainLandBoundary = new Envelope(-126.7901resX, -64.630926, 24.863836, 50.000)
    //  val HDFIncrement = 5
    //  val HDFOffset = 2
    //  val HDFRootGroupName = "MOD_Swath_LST"
    //  val HDFDataVariableName = "LST"
    //  val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")
    //  val HDFswitchXY = true
    //  val urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/"

    //    val geometryFactory = new GeometryFactory()
    //    val inputGenerator = GenerateUniformData()
    //    //    val dataBoundary = new Envelope(0, 1000, 0, 1000)
    //    val inputRDD = inputGenerator.generate(1000, 1000, sparkContext)
    //    val queryRDD = inputGenerator.generate(1000, 1000, sparkContext)
    //    val solPoint = queryRDD.rawSpatialRDD.rdd.reduce((p1, p2) =>
    //      geometryFactory.createPoint(new Coordinate(
    //        (p1.getX + p2.getX) / 2.0,
    //        (p1.getY + p2.getY) / 2.0
    //      )))

    //    buildScatterPlot(sparkContext, geometryFactory, inputRDD, queryRDD,
    //      solPoint,
    //      scatterPlotOutputPath)

    /**
      * Builds the heat map.
      *
      * @param outputPath the output path
      * @return true, if successful
      */
    /**
    * def buildHeatMap(spatialRDD1: SpatialRDD[_ <:Geometry],
    * spatialRDD2: SpatialRDD[_ <:Geometry],
    * dataBoundary: Envelope,
    * outputPath: String): Boolean = {
    * val visualizationOperator1 = new HeatMap(1000, resY, dataBoundary, false, 2)
    * visualizationOperator1.CustomizeColor(0, 0, 255, 255, Color.BLUE, false)
    * visualizationOperator1.Visualize(sparkContext, spatialRDD1)
    * *
    * val visualizationOperator2 = new HeatMap(1000, resY, dataBoundary, false, 2)
    * visualizationOperator2.CustomizeColor(0, 255, 0, 255, Color.GREEN, false)
    * visualizationOperator2.Visualize(sparkContext, spatialRDD2)
    * *
    * val w = Math.max(visualizationOperator1.rasterImage.getWidth,
    * visualizationOperator2.rasterImage.getWidth)
    * val h = Math.max(visualizationOperator1.rasterImage.getHeight,
    * visualizationOperator2.rasterImage.getHeight)
    * *
    * val mergedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB)
    * val g = mergedImage.getGraphics
    *g.drawImage (visualizationOperator1.rasterImage, 0, 0, null)
    *g.drawImage (visualizationOperator2.rasterImage, 0, 0, null)
    * *
    * val imageGenerator = new ImageGenerator
    *imageGenerator.SaveRasterImageAsLocalFile(mergedImage, outputPath, ImageType.PNG)
    * true
    * }
    */
  }

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
    val RES_X = 100
    val RES_Y = 100
    val COLORS = List(
      (Color.CYAN, Color.BLUE),
      (Color.ORANGE, Color.RED),
      (Color.RED, Color.RED),
      (Color.GREEN, Color.GREEN),
      (Color.MAGENTA, Color.BLUE),
      (Color.YELLOW, Color.GREEN)
    )

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
