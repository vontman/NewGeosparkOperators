package example

import java.awt.Color
import java.awt.image.BufferedImage
import java.nio.file.Paths

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import org.datasyslab.geosparkviz.core.ImageGenerator
import org.datasyslab.geosparkviz.extension.visualizationEffect.ScatterPlot
import org.datasyslab.geosparkviz.utils.ImageType

object Plotter {
  val geometryFactory: GeometryFactory = new GeometryFactory()

  def visualizeNaiive(sc: JavaSparkContext, pointRDD: PointRDD, plotName: String): Unit = {

    val scatterOutput = Paths.get("visualization", plotName).toString


    val resX = 800
    val resY = 800

    val dataOperator = new ScatterPlot(resX, resY, pointRDD.boundaryEnvelope, false, false)
    dataOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
    dataOperator.Visualize(sc, pointRDD)

    val afterImage = new BufferedImage(resX, resY, BufferedImage.TYPE_INT_ARGB)
    val afterImageG = afterImage.getGraphics
    afterImageG.setColor(Color.BLACK)
    afterImageG.fillRect(0, 0, resX, resY)
    afterImageG.drawImage(dataOperator.rasterImage, 0, 0, null)

    val imageGenerator = new ImageGenerator()
    imageGenerator.SaveRasterImageAsLocalFile(afterImage, scatterOutput, ImageType.PNG)
  }

  def visualize(sc: JavaSparkContext,
                candidatePoints: PointRDD,
                plotName: String,
                totalPlotBounds: Envelope,
                filteredPoints: PointRDD = null,
                partitions: List[PartitionProps] = null): Unit = {

    val scatterOutput = Paths.get("visualization", plotName).toString


    val resX = 300
    val resY = 300

    var grids: PolygonRDD = null
    if (partitions != null) {

      grids = new PolygonRDD(sc.parallelize(partitions.map(_.envelop).map(env => {
        geometryFactory.createPolygon(Array(
          new Coordinate(env.getMinX, env.getMinY),
          new Coordinate(env.getMinX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMinY),
          new Coordinate(env.getMinX, env.getMinY)
        ))
      })))

      grids.analyze()
    }


    val plotBounds = {
      if (totalPlotBounds == null) {
        val env = new Envelope(candidatePoints.boundaryEnvelope)
        if (filteredPoints != null) {
          env.expandToInclude(filteredPoints.boundaryEnvelope)
        }
        if (partitions != null) {
          partitions.map(_.envelop).foreach(env.expandToInclude)
        }
        env
      } else {
        totalPlotBounds
      }
    }

    val candidatesOperator = new ScatterPlot(resX, resY, plotBounds, false, false)
    candidatesOperator.CustomizeColor(Color.CYAN.getRed, Color.CYAN.getGreen, Color.CYAN.getBlue, 255, Color.BLUE, false)
    candidatesOperator.Visualize(sc, candidatePoints)

    var boundsOperator: ScatterPlot = null
    if (partitions != null) {
      boundsOperator = new ScatterPlot(resX, resY, plotBounds, false, false)
      boundsOperator.CustomizeColor(Color.RED.getRed, Color.RED.getGreen, Color.RED.getBlue, 255, Color.RED, false)
      boundsOperator.Visualize(sc, grids)
    }

    var filteredOperator: ScatterPlot = null
    if (filteredPoints != null) {
      filteredOperator = new ScatterPlot(resX, resY, plotBounds, false, false)
      filteredOperator.CustomizeColor(Color.YELLOW.getRed, Color.YELLOW.getGreen, Color.YELLOW.getBlue, 255, Color.GREEN, false)
      filteredOperator.Visualize(sc, filteredPoints)
    }


    val afterImage = new BufferedImage(resX, resY, BufferedImage.TYPE_INT_ARGB)
    val afterImageG = afterImage.getGraphics
    afterImageG.setColor(Color.BLACK)
    afterImageG.fillRect(0, 0, resX, resY)
    afterImageG.drawImage(candidatesOperator.rasterImage, 0, 0, null)

    if (filteredPoints != null) {
      afterImageG.drawImage(filteredOperator.rasterImage, 0, 0, null)
    }

    if (partitions != null) {
      afterImageG.drawImage(boundsOperator.rasterImage, 0, 0, null)
    }

    val imageGenerator = new ImageGenerator()
    imageGenerator.SaveRasterImageAsLocalFile(afterImage, scatterOutput, ImageType.PNG)
  }

  def plotPartitions(sc: JavaSparkContext, partitions: List[PartitionProps], plotName: String): Unit = {

    val scatterOutput = Paths.get("visualization", "partitions_boarders", plotName).toString


    val resX = 300
    val resY = 300

    val grids: PolygonRDD = new PolygonRDD(sc.parallelize(
      partitions.map(partition => {
        val env = partition.envelop

        geometryFactory.createPolygon(Array(
          new Coordinate(env.getMinX, env.getMinY),
          new Coordinate(env.getMinX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMinY),
          new Coordinate(env.getMinX, env.getMinY)
        ))
      })))

    grids.analyze()

    val boundsOperator = new ScatterPlot(resX, resY, grids.boundaryEnvelope, false, false)
    boundsOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)

    boundsOperator.Visualize(sc, grids)


    val afterImage = new BufferedImage(resX, resY, BufferedImage.TYPE_INT_ARGB)
    val afterImageG = afterImage.getGraphics
    afterImageG.setColor(Color.BLACK)
    afterImageG.fillRect(0, 0, resX, resY)

    afterImageG.drawImage(boundsOperator.rasterImage, 0, 0, null)


    val imageGenerator = new ImageGenerator()
    imageGenerator.SaveRasterImageAsLocalFile(afterImage, scatterOutput, ImageType.PNG)
  }
}
