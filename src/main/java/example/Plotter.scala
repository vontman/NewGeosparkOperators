package example

import java.awt.Color
import java.awt.image.BufferedImage
import java.nio.file.Paths

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.api.java.JavaSparkContext
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import org.datasyslab.geosparkviz.core.ImageGenerator
import org.datasyslab.geosparkviz.extension.visualizationEffect.ScatterPlot
import org.datasyslab.geosparkviz.utils.ImageType

object Plotter {
  val geometryFactory: GeometryFactory = new GeometryFactory()

  def visualize(sc: JavaSparkContext, pointRDD: PointRDD, plotName: String): Unit = {

    val scatterOutput = Paths.get("visualization", plotName).toString


    val resX = 600
    val resY = 600

    val grids = new PolygonRDD(
      pointRDD.indexedRDD.rdd.filter((index: SpatialIndex) => {
        index.asInstanceOf[STRtree].getRoot.getBounds != null
      }).map((index: SpatialIndex) => {
        val env = index.asInstanceOf[STRtree].getRoot.getBounds.asInstanceOf[Envelope]

        geometryFactory.createPolygon(geometryFactory.createLinearRing(Array(
          new Coordinate(env.getMinX, env.getMinY),
          new Coordinate(env.getMinX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMinY),
          new Coordinate(env.getMinX, env.getMinY)
        )))
      }))

    grids.analyze()

    val dataOperator = new ScatterPlot(resX, resY, pointRDD.boundaryEnvelope, false, false)
    dataOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
    dataOperator.Visualize(sc, pointRDD)

    val boundsOperator = new ScatterPlot(resX, resY, grids.boundaryEnvelope, false, false)
    boundsOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    boundsOperator.Visualize(sc, grids)


    val afterImage = new BufferedImage(resX, resY, BufferedImage.TYPE_INT_ARGB)
    val afterImageG = afterImage.getGraphics
    afterImageG.setColor(Color.BLACK)
    afterImageG.fillRect(0, 0, resX, resY)
    afterImageG.drawImage(dataOperator.rasterImage, 0, 0, null)
    afterImageG.drawImage(boundsOperator.rasterImage, 0, 0, null)

    val imageGenerator = new ImageGenerator()
    imageGenerator.SaveRasterImageAsLocalFile(afterImage, scatterOutput, ImageType.PNG)
  }
}
