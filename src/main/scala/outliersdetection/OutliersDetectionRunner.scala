
package outliersdetection

import java.io.File

import org.datasyslab.geospark.enums.{GridType, IndexType}
import utils.{GenerateGaussianData, SparkRunner}

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionRunner {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sc = SparkRunner.start()

    deleteOldValidation()
    for (iter <- 0 to 70) {
      val data = GenerateGaussianData().generate(sc, 10000, 800000)

      val n = 100
      val k = 100

      data.analyze
      var nextRdd = data
      var prevCount = 0L
      var nextCount = 0L
      var pruningIteration = 1
      val originalBounds = data.boundaryEnvelope

      data.spatialPartitioning(GridType.RTREE)
      data.buildIndex(IndexType.RTREE, true)

      Array(
        //        new ExpanderByTotalPointsRatio(0.3, 5000)
        new ExpanderByPointsRatioPerGrid(0.4, 5000, x => x.getBounds.getArea)
        //        new ExpanderWithAreaBounds(0.2, 5000, 1.0 / 300, 1.0 / 5000, indexNode => indexNode.getBounds.getArea)
      ).foreach(expander => {
        //        for (_ <- 0 until 1) {
        prevCount = nextRdd.countWithoutDuplicates
        nextRdd = OutliersDetectionGeneric(GridType.QUADTREE, IndexType.QUADTREE, expander).findOutliers(originalBounds, nextRdd, n, k, s"visualization/$iter/${expander.getClass.getSimpleName}_$pruningIteration")._2
        nextCount = nextRdd.countWithoutDuplicates
        println("Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")

        pruningIteration += 1
        //        }


        val possibleAns = nextRdd.rawSpatialRDD.collect().toList

        if (possibleAns.size < data.approximateTotalCount) {
          val ans = OutliersDetectionNaiive.findOutliersNaive(data, k, n)
          println(s"Finished Naiive Execution and found ${ans.size} outliers")
          Plotter.visualizeNaiive(sc, data.boundaryEnvelope, ans, "natiive")
          if (possibleAns.containsAll(ans)) {
            println(s"$iter VALID")
          }
          else {
            println(s"$iter INVALID")
          }
        }
      })
    }
    sc.stop
  }

  private def deleteOldValidation() = {
    val visualizationsFile = new File("visualization")
    if (visualizationsFile.exists()) {
      System.out.println("Delete old visualizations")
      val process = Runtime.getRuntime.exec(s"rm -rf ${visualizationsFile.getAbsolutePath}")
      process.waitFor
    }
  }
}