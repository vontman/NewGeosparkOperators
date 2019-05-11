package outliersdetection

import java.io.File

import com.bizo.mighty.csv.CSVDictWriter
import com.vividsolutions.jts.geom.Point
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import utils.{
  GenerateExponentialData,
  GenerateGuassianData,
  GenerateNonUniformData,
  GenerateUniformData,
  GenerateZipfData
}

import scala.language.postfixOps
import scala.util.Random

object OutliersDetectionBenchmark {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("GeoSparkRunnableExample")
      .setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator",
      classOf[GeoSparkVizKryoRegistrator].getName)
    val sc = new JavaSparkContext(conf)

    val outputPath = s"benchmark/${System.currentTimeMillis()}/"

    new File(outputPath).mkdirs()
    val headers = Seq(
      "name",
      "dataCount",
      "n",
      "k",
      "gridType",
      "indexType",
      "input_generation_strategy",
      "iteration_1_used_partitions",
      "iteration_1_partitions_after_pruning",
      "iteration_1_points_after_pruning",
      "iteration_1_pruning_percentage",
      "iteration_1_time",
      "iteration_2_used_partitions",
      "iteration_2_partitions_after_pruning",
      "iteration_2_points_after_pruning",
      "iteration_2_pruning_percentage",
      "iteration_2_time"
    )

    val defLog = headers.map(_ -> "0").toMap

    val resultsCsv = CSVDictWriter(outputPath + "results.csv", headers)
    resultsCsv.writeHeader()

    for {
      inputGenerationStrategy <- List(
                GenerateUniformData(),
        GenerateGuassianData(),
        GenerateExponentialData(),
        GenerateNonUniformData(),
        GenerateZipfData(.75),
        GenerateZipfData(.9))

      (dataCount, n, k, maxIterations) <- List(
        (50000, 100, 100, 1)
        //        (10000, 100, 100, 20),
        //        (50000, 100, 100, 20),
        //        (100000, 300, 200, 10),
        //        (250000, 300, 300, 10),
        //        (500000, 300, 300, 5),
        //        (1000000, 500, 700, 3)
      )

      iteration <- 1 to maxIterations

      gridType <- List(GridType.QUADTREE, GridType.RTREE)
      indexType = IndexType.QUADTREE

    } {
      val dataRDD = inputGenerationStrategy.generate(dataCount, 100000, sc)
      val id = Random.nextLong()

      for {

        (expansionFunction, solverName) <- List(
          (
            new ExpanderWithAreaBounds(.1,
              500,
              0.003,
              0.00005,
              node => node.getBounds.getArea),
            "ExpanderWithAreaBounds_.1_10k_.003_.0003"
          ),
          (
            new ExpanderByTotalPointsRatio(.1, 500),
            "ExpanderByTotalPointsRatio_.1_10k_.003_.0003"
          ),
          (
            new ExpanderByPointsRatioPerGrid(.1,
              500,
              node => node.getBounds.getArea),
            "ExpanderByPointsRatioPerGrid_.1_10k_.003_.0003"
          )
        )

      } {
        println(
          s"Starting a new test iteration: $iteration/$maxIterations, dataCount: $dataCount, solver: $solverName, inputGen: ${inputGenerationStrategy.getClass.getSimpleName}")

        var currDataRDD = dataRDD

        var logger = defLog

        for (iter <- 1 to 2) {
          val (logs, filteredRDD) =
            OutliersDetectionGeneric(gridType, indexType, expansionFunction)
              .findOutliers(
                currDataRDD,
                n,
                k,
                s"$outputPath/${id}_${solverName}_${inputGenerationStrategy.getClass.getSimpleName}_iteration_${iter}_")
          val newPointsCount = filteredRDD.rawSpatialRDD.count()

          logger ++=
            Map(
              "name" -> solverName,
              "dataCount" -> dataCount.toString,
              "n" -> n.toString,
              "k" -> k.toString,
              "input_generation_strategy" -> inputGenerationStrategy.getClass.getSimpleName,
              "gridType" -> gridType.toString,
              "indexType" -> indexType.toString,
              s"iteration_${iter}_used_partitions" -> logs("used_partitions"),
              s"iteration_${iter}_partitions_after_pruning" -> logs(
                "partitions_after_pruning"),
              s"iteration_${iter}_points_after_pruning" -> newPointsCount.toString,
              s"iteration_${iter}_pruning_percentage" -> (100.0 * (dataCount - newPointsCount) / dataCount).toString,
              s"iteration_${iter}_time" -> logs("time")
            )

          currDataRDD = filteredRDD
        }

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
