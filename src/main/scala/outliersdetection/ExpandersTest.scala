package outliersdetection

import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

import scala.collection.mutable

import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.KNNQuery
import utils._

import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Random


object ExpandersTest {

  def main(args: Array[String]) = {

    val sc = SparkRunner.start()

    var errFlag = false

    var iterations = 0

    for {
      //_ <- 0 until 10
      dataGenerator <- List(
        GenerateUniformData(),
        GenerateExponentialData(),
        GenerateGaussianData(),
        GenerateZipfData(.25),
        GenerateZipfData(.5),
        GenerateZipfData(.9),
        GenerateNonUniformData()
      )
      (expansionFunction, expanderName) <- ExpanderWithAreaBounds.getPermutations ::: ExpanderByPointsRatioPerGrid.getPermutations ::: ExpanderByPointsRatioPerGrid.getPermutations
      } {

        iterations += 1

        println(s"Now testing $expanderName")
        val n = 10000
        val data = dataGenerator.generate(sc, n, 800000)

        data.analyze()
        data.spatialPartitioning(GridType.QUADTREE)
        data.buildIndex(IndexType.QUADTREE, true)

        val expandedData = expansionFunction.expand(data)

        val pointsCount = expandedData.map(_.getPointsCount).sum()

        if (n != pointsCount) {
          errFlag = true
          println(s"ERR Total Points Count: $pointsCount")
        }

        if (
          expandedData.filter(
            node => {
              val children = node.getChildren
              if (children.isEmpty) {
                false
              } else {
                val env = node.getBounds
                val points = node.getAllPoints
                points.exists(p => !env.contains(p.getCoordinate))
              }
            }
            ).count > 0
          ) {
            errFlag = true
            println(s"ERR INVALID BOUNDS")
          }

      }


      println(s"Did $iterations iterations")
      if (errFlag) {
        println("ERR Found")
      } else {
        println("ALL GOOD")
      }

      sc.stop()

  }
}
