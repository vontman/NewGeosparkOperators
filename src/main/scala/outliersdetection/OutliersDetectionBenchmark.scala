package outliersdetection

import java.io.File
import java.util.concurrent.TimeoutException

import com.bizo.mighty.csv.CSVDictWriter
import com.vividsolutions.jts.geom.Point
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import utils.{GenerateExponentialData, GenerateGuassianData, GenerateNonUniformData, GenerateUniformData, GenerateZipfData, Visualization}

import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object OutliersDetectionBenchmark {

  def runWithTimeout[T](timeoutMs: Long)(f: => T): Option[T] = {
    try {
      Some(Await.result(Future(f), timeoutMs milliseconds))
    } catch {
      case _: TimeoutException => None
      case e: Exception =>
        println(e.getMessage)
        e.printStackTrace()
        None
    }
  }

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
//                GenerateUniformData(),
        GenerateGuassianData(),
        GenerateExponentialData(),
        GenerateNonUniformData(),
        GenerateZipfData(.75),
        GenerateZipfData(.9)
                )

      (dataCount, n, k, maxIterations) <- List(
//        (50000, 100, 100, 10)
                (10000, 100, 100, 20),
                (50000, 100, 100, 20),
                (100000, 300, 200, 10),
                (250000, 300, 300, 10),
                (500000, 300, 300, 5),
                (1000000, 500, 700, 3)
      )

      iteration <- 1 to maxIterations

    } {
      val dataRDD = inputGenerationStrategy.generate(dataCount, 100000, sc)
//      val dataRDD = {
//        val location = "benchmark/1557588114229/2640218211505695195_RTREE_ExpanderWithAreaBounds_.1_10k_.003_.0003_RTREE_100_ 100_GenerateExponentialData_data"
//        val splitter = FileDataSplitter.GEOJSON
//        val offset = 0
//        new PointRDD(sc,
//          location,
//          offset,
//          splitter,
//          true)
//      }
      val id = Random.nextLong()

      for {
        gridType <- List(GridType.QUADTREE, GridType.RTREE)
        indexType = IndexType.QUADTREE

//        (expansionFunction, expanderName) <- ExpanderWithAreaBounds.getPermutations ::: ExpanderByPointsRatioPerGrid.getPermutations ::: ExpanderByPointsRatioPerGrid.getPermutations
        (expansionFunction, expanderName) <- ExpanderByTotalPointsRatio.getPermutations ::: ExpanderByPointsRatioPerGrid.getPermutations

        solverName = s"${gridType}_${expanderName}"
      } {
        println(
          s"Starting a new test iteration: $iteration/$maxIterations, dataCount: $dataCount, solver: $solverName, inputGen: ${inputGenerationStrategy.getClass.getSimpleName}")

        var currDataRDD = dataRDD

        var logger = defLog

        for (iter <- 1 to 2) {

          val (logs, filteredRDD) = {

            val ret = runWithTimeout(240000) {
              OutliersDetectionGeneric(gridType, indexType, expansionFunction)
                .findOutliers(
                  currDataRDD,
                  n,
                  k,
                  s"$outputPath/${id}_${solverName}_${inputGenerationStrategy.getClass.getSimpleName}_iteration_${iter}_")
            }

            ret match {
              case Some(res) => res
              case None =>
                dataRDD.saveAsGeoJSON(
                s"$outputPath/timeouts/${id}_${solverName}_${gridType}_${k}_ ${n}_${inputGenerationStrategy.getClass.getSimpleName}_data"
                )
                Visualization.buildScatterPlot(
                  List(dataRDD),
                  s"$outputPath/timeouts/${id}_${solverName}_${gridType}_${k}_ ${n}_${inputGenerationStrategy.getClass.getSimpleName}_plot"
                )

                (defLog, currDataRDD)
            }


          }
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
              s"iteration_${iter}_used_partitions" -> logs.getOrElse("used_partitions", "0"),
              s"iteration_${iter}_partitions_after_pruning" -> logs.getOrElse(
                "partitions_after_pruning", "0"),
              s"iteration_${iter}_points_after_pruning" -> newPointsCount.toString,
              s"iteration_${iter}_pruning_percentage" -> (100.0 * (dataCount - newPointsCount) / dataCount).toString,
              s"iteration_${iter}_time" -> logs.getOrElse("time", "120000")
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
