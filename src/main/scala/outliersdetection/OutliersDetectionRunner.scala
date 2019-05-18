
package outliersdetection

import java.io.File

import org.datasyslab.geospark.enums.{GridType, IndexType}
import utils.{GenerateUniformData, SparkRunner}

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionRunner {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sc = SparkRunner.start()

    deleteOldValidation()
    for (iter <- 0 to 1000) {
      val data = GenerateUniformData().generate(sc, 50, 1000)

      val n = 1
      val k = 5

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
        new ExpanderByPointsRatioPerGrid(0.5, 1000, x => x.getBounds.getArea)
        //        new ExpanderWithAreaBounds(0.4, 5000, 1.0 / 300, 1.0 / 5000, indexNode => indexNode.getBounds.getArea)
      ).foreach(expander => {
        //        for (_ <- 0 until 1) {
        prevCount = nextRdd.countWithoutDuplicates
        nextRdd = OutliersDetectionGeneric(GridType.RTREE, IndexType.RTREE, expander).findOutliers(originalBounds, nextRdd, n, k, s"visualization/$iter/${expander.getClass.getSimpleName}_$pruningIteration")._2
        nextCount = nextRdd.countWithoutDuplicates
        println("Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")

        pruningIteration += 1
        //        }


        val possibleAns = nextRdd.rawSpatialRDD.collect().toList

        if (possibleAns.size < data.approximateTotalCount) {
          val ans = OutliersDetectionNaiive.findOutliersNaive(data, n, k)
          println(s"Finished Naiive Execution and found ${ans.size} outliers")
          try {
            Plotter.visualizeNaiive(sc, data.boundaryEnvelope, ans, s"visualization/$iter/naiive")
          } catch {
            case e: Exception => println(s"$iter Could not plot naiive solution")
          }
          if (ans.exists(possibleAns.contains)) {
            println(s"$iter VALID\n")
          } else {
            println(s"$iter INVALID")
            println(s"Naiive   Ans -> [${ans.mkString(", ")}]")
            println(s"Operator Ans -> [${possibleAns.mkString(", ")}]")
            println(s"Difference   -> [${ans.filterNot(possibleAns.contains).mkString(", ")}]\n")
            System.exit(-1)
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