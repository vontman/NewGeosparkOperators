package outliersdetection

import java.io.File

import org.datasyslab.geospark.enums.{GridType, IndexType}
import utils.{GenerateGaussianData, GenerateNonUniformData, GenerateUniformData, GenerateZipfData, SparkRunner}

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionRunner {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sc = SparkRunner.start()

    deleteOldValidation()

    for (iter <- 0 to 1000) {
      val data = GenerateNonUniformData().generate(sc, 100000, 100000, numPartitions = 4)

      val n = 300
      val k = 300

      data.analyze
      var nextRdd = data
      var prevCount = 0L
      var nextCount = 0L
      var pruningIteration = 1
      val originalBounds = data.boundaryEnvelope

      data.spatialPartitioning(GridType.QUADTREE)
      data.buildIndex(IndexType.QUADTREE, true)

      Array(
        //        new ExpanderByTotalPointsRatio(0.3, 5000),
        //new ExpanderByPointsRatioPerGrid(0.7, 7000, x => x.getBounds.getArea),
        new ExpanderWithAreaBounds(
          0.1,
          10000,
          1.0 / 300,
          1.0 / 5000,
          indexNode => indexNode.getBounds.getArea)
//      new ExpanderWithAreaBounds(.5, 5000, 1.0 / 300, 1.0 / 5000,  indexNode => indexNode.getBounds.getArea)
      ).foreach(expander => {

        var t0 = 0L

        t0 = System.currentTimeMillis()
        prevCount = nextRdd.countWithoutDuplicates
        nextRdd = OutliersDetectionGeneric(GridType.RTREE,
                                           IndexType.RTREE,
                                           expander)
          .findOutliers(
            originalBounds,
            data,
            n,
            k,
            s"visualization/$iter/${expander.getClass.getSimpleName}_$pruningIteration")
          ._2
        nextCount = nextRdd.countWithoutDuplicates
        println(s"TimeElapsed: ${System.currentTimeMillis() - t0} millis")
        println(
          "Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")

        pruningIteration += 1

        val possibleAns = nextRdd.rawSpatialRDD.collect().toList

        if (false && possibleAns.size < data.approximateTotalCount) {

          t0 = System.currentTimeMillis()
          val result =
            OutliersDetectionNaiveWithKNNJoin.findOutliersNaive3(nextRdd,
                                                                 k,
                                                                 n,
                                                                 data)
          println(s"Result TimeElapsed: ${System.currentTimeMillis() - t0} millis")

          t0 = System.currentTimeMillis()
          val ans = OutliersDetectionNaiveWithKNNJoin.findOutliersNaive3(data,
                                                                         k,
                                                                         n,
                                                                         data)
          println(s"Naive TimeElapsed: ${System.currentTimeMillis() - t0} millis")
          println(s"Finished Naiive Execution and found ${ans.size} outliers")
          if (ans.map(_._2).containsAll(result.map(_._2)) && result
                .map(_._2)
                .containsAll(ans.map(_._2))) {
            println(s"$iter VALID\n")
          } else {
            try {
              Plotter.visualizeNaiive(sc,
                                      data.boundaryEnvelope,
                                      ans.map(_._1),
                                      s"visualization/$iter/naiive")
            } catch {
              case e: Exception =>
                println(s"$iter Could not plot naiive solution")
            }
            println(s"$iter INVALID")
            println(s"Naiive   Ans -> [${ans.map(_._2).mkString(", ")}]")
            println(s"Operator Ans -> [${result.map(_._2).mkString(", ")}]")
            println(
              s"Difference   -> [${ans.map(_._2).diff(result.map(_._2)).mkString(", ")}]\n")
            System.exit(-1)
          }
        }
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
