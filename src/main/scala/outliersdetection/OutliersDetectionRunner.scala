package outliersdetection

import java.io.File

import org.datasyslab.geospark.enums.{GridType, IndexType}
import utils.{GenerateNonUniformData, GenerateUniformData, SparkRunner}

import scala.language.postfixOps

object OutliersDetectionRunner {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sc = SparkRunner.start()

    deleteOldValidation()

    for (iter <- 0 to 1000) {
      val data = GenerateUniformData().generate(sc, 50000, 100000, numPartitions = 4)

      val n = 100
      val k = 100

      data.analyze
      //      var nextRdd = data
      //      var prevCount = 0L
      //      var nextCount = 0L
      var pruningIteration = 1
      val originalBounds = data.boundaryEnvelope

      data.spatialPartitioning(GridType.QUADTREE)
      data.buildIndex(IndexType.QUADTREE, true)

      Array(
        new ExpanderByPointsRatioPerGrid(0.1, 700000, x => x.getBounds.getArea)
//        new ExpanderByPointsRatioPerGrid(0.1, 700000, x => x.getPointsCount),
//        new ExpanderByPointsRatioPerGrid(0.1, 700000, indexNode => indexNode.getPointsCount / indexNode.getBounds.getArea),
//        new ExpanderWithAreaBounds(
//          0.1,
//          1000000,
//          1.0 / 300,
//          1.0 / 5000,
//          indexNode => indexNode.getBounds.getArea),
//        new ExpanderWithAreaBounds(.5, 5000, 1.0 / 300, 1.0 / 5000, indexNode => indexNode.getBounds.getArea),
//      new ExpanderByTotalPointsRatio(0.1, 500000)
      ).foreach(expander => {

        val (genericLogs, genericAns) = OutliersDetectionGeneric(GridType.QUADTREE,
          IndexType.QUADTREE,
          expander)
          .findOutliers(
            originalBounds,
            data,
            n,
            k,
            s"visualization/$iter/${expander.getClass.getSimpleName}_$pruningIteration")

        println("Generic")
        println(genericLogs.mkString("\n"))
        println()



        val (knnJoinLogs, knnJoinAns) = OutliersDetectionNaiveWithKNNJoin.findOutliersNaive(data, k, n)
        println("Naive knn join")
        println(knnJoinLogs.mkString("\n"))
        println()


        if (knnJoinAns != genericAns) {
          println("Mismatch in answer")
          println(s"Diff: ${knnJoinAns.diff(genericAns)}")
        }
        pruningIteration += 1

      })
    }
    sc.getPersistentRDDs.foreach(_._2.unpersist())
    sc.stop
  }

  private def deleteOldValidation() = {
    val visualizationsFile = new File("visualization")
    if (visualizationsFile.exists()) {
      System.out.println("Delete old visualizations")
      val process =
        Runtime.getRuntime.exec(s"rm -rf ${visualizationsFile.getAbsolutePath}")
      process.waitFor
    }
  }
}
