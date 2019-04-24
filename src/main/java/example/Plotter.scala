package example

import java.awt.Color
import java.awt.image.BufferedImage
import java.nio.file.Paths

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree.{NodeBase, Quadtree}
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.api.java.JavaSparkContext
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import org.datasyslab.geosparkviz.core.ImageGenerator
import org.datasyslab.geosparkviz.extension.visualizationEffect.ScatterPlot
import org.datasyslab.geosparkviz.utils.ImageType

object Plotter {
  val geometryFactory: GeometryFactory = new GeometryFactory()

  def visualizeR(sc: JavaSparkContext, pointRDD: PointRDD, plotName: String, withBoarders: Boolean = true): Unit = {

    val scatterOutput = Paths.get("visualization", plotName).toString


    val resX = 300
    val resY = 300

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
      }).cache())

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

    if (withBoarders) {
      afterImageG.drawImage(boundsOperator.rasterImage, 0, 0, null)
    }

    val imageGenerator = new ImageGenerator()
    imageGenerator.SaveRasterImageAsLocalFile(afterImage, scatterOutput, ImageType.PNG)
  }

  def visualizeQuad(sc: JavaSparkContext, pointRDD: PointRDD, plotName: String, withBoarders: Boolean = true): Unit = {

    val scatterOutput = Paths.get("visualization", plotName).toString


    val resX = 300
    val resY = 300

    val grids = new PolygonRDD(
      pointRDD.indexedRDD.rdd.filter((index: SpatialIndex) => {
        index.asInstanceOf[Quadtree].getRoot.asInstanceOf[NodeBase].hasChildren
      }).map((index: SpatialIndex) => {
        val env = new Envelope()
        for (node <- index.asInstanceOf[Quadtree].getRoot.getSubnode) {
          if (node != null) {
            if (env.isNull) {
              env.init(node.getEnvelope)
            } else {
              env.expandToInclude(node.getEnvelope)
            }
          }
        }

        val geometryFactory = new GeometryFactory()
        geometryFactory.createPolygon(geometryFactory.createLinearRing(Array(
          new Coordinate(env.getMinX, env.getMinY),
          new Coordinate(env.getMinX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMinY),
          new Coordinate(env.getMinX, env.getMinY)
        )))
      }).cache())

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

    if (withBoarders) {
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

        geometryFactory.createPolygon(geometryFactory.createLinearRing(Array(
          new Coordinate(env.getMinX, env.getMinY),
          new Coordinate(env.getMinX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMaxY),
          new Coordinate(env.getMaxX, env.getMinY),
          new Coordinate(env.getMinX, env.getMinY)
        )))
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
