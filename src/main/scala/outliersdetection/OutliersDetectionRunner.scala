
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
      val data = GenerateUniformData().generate(sc, 10000, 100000)

      val n = 50
      val k = 50

      data.analyze
      val originalBounds = data.boundaryEnvelope

      data.spatialPartitioning(GridType.RTREE)
      data.buildIndex(IndexType.RTREE, true)

      Array(
        //        new ExpanderByTotalPointsRatio(0.3, 5000),
        //new ExpanderByPointsRatioPerGrid(0.7, 7000, x => x.getBounds.getArea),
        new ExpanderWithAreaBounds(0.7, 7000, 1.0 / 300, 1.0 / 5000, indexNode => indexNode.getPointsCount / indexNode.getBounds.getArea)
      ).foreach(expander => {
        val ans = OutliersDetectionGeneric(GridType.RTREE, IndexType.RTREE, expander).findOutliers(originalBounds, data, n, k, s"visualization/$iter/${expander.getClass.getSimpleName}")._2

        val verifiedAns = OutliersDetectionNaiive.findOutliersNaive(data, n, k)


        if (verifiedAns.containsAll(ans) && ans.containsAll(verifiedAns)) {
          println(s"$iter VALID\n")
        } else {
          println(s"$iter INVALID")
          println(s"Naiive   Ans -> [${verifiedAns.mkString(", ")}]")
          println(s"Operator Ans -> [${ans.mkString(", ")}]")
          println(s"Difference   -> [${(verifiedAns.filterNot(ans.contains) ++ ans.filterNot(verifiedAns.contains)).mkString(", ")}]\n")
          System.exit(-1)
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
