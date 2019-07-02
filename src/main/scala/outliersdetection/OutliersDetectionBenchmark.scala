package outliersdetection

import java.io.File

import com.bizo.mighty.csv.CSVDictWriter
import org.datasyslab.geospark.enums.{GridType, IndexType}
import utils._

import scala.language.postfixOps
import scala.util.Random

object OutliersDetectionBenchmark {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sc = SparkRunner.start()

    val outputPath = s"benchmark/${System.currentTimeMillis()}/"

    new File(outputPath).mkdirs()
    val headers = Seq(
      "name",
      "n",
      "k",
      "gridType",
      "indexType",
      "input_generation_strategy",
      "total_time",
      "used_partitions",
      "expanding_partitions_time",
      "pruning_time",
      "reducing_outliers_time",
      "neighbour_partitions",
      "candidate_partitions",
      "pruned_partitions",
      "neighbours_percentage",
      "pruning_percentage",
      "reducing_outliers_time_knnjoin",
      "reducing_outliers_time_custom"
    )


    val defLog = headers.map(_ -> "0").toMap

    val resultsCsv = CSVDictWriter(outputPath + "results.csv", headers)
    resultsCsv.writeHeader()

    for {
      inputGenerationStrategy <- List(
        //        GenerateUniformData(),
        //        GenerateGaussianData(),
        //        GenerateExponentialData(),
        //        GenerateNonUniformData(),
        //        GenerateZipfData(.75)
        //        GenerateZipfData(.9),
        GenerateRandomGaussianClusters(15, 1000, 10000)
      )

      (dataCount, n, k) <- List(
        (0, 100, 50),
        (0, 100, 100),
        (0, 300, 200),
        (0, 300, 300),
        (0, 300, 300),
        (0, 500, 700)
      )
    } {
      val dataRDD = inputGenerationStrategy.generate(sc, dataCount, 10000000)

      val id = Random.nextLong()

      for {
        gridType <- List(GridType.QUADTREE, GridType.RTREE)
        indexType <- List(IndexType.RTREE, IndexType.QUADTREE)

        (expansionFunction, expanderName) <- ExpanderWithAreaBounds.getPermutations ::: ExpanderByPointsRatioPerGrid.getPermutations ::: ExpanderByTotalPointsRatio.getPermutations

        solverName = s"${gridType}_$expanderName"
      } {
        println(s"Starting a new test iteration: solver: $solverName, inputGen: ${inputGenerationStrategy.getClass.getSimpleName}")

        val originalBounds = dataRDD.boundaryEnvelope

        var logger = defLog

        val (logs, _) =
          OutliersDetectionGeneric(gridType, indexType, expansionFunction)
            .findOutliers(
              originalBounds,
              dataRDD,
              n,
              k,
              s"$outputPath/${id}_${solverName}_${inputGenerationStrategy.getClass.getSimpleName}")

        logger ++=
          Map(
            "name" -> solverName,
            "n" -> n.toString,
            "k" -> k.toString,
            "input_generation_strategy" -> inputGenerationStrategy.getClass.getSimpleName,
            "gridType" -> gridType.toString,
            "indexType" -> indexType.toString
          ) ++ logs

        resultsCsv.write(logger)
        resultsCsv.flush()

      }

    }

    resultsCsv.close()
    sc.stop
  }

  //
  //  private def deleteOldValidation() = {
  //    System.out.println("Delete old visualizations")
  //    val visualizationsFile = new File("visualization/outliers")
  //    val process = Runtime.getRuntime.exec(
  //      String.format("rm -rf %s", visualizationsFile.getAbsolutePath))
  //    process.waitFor
  //  }
}
