package gnn

import java.io.{File, PrintWriter}

import com.bizo.mighty.csv.CSVDictWriter
import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import utils._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random

/**
  * The Class ScalaExample.
  */
class GNNBenchmark(sparkContext: SparkContext,
                   visualizeResult: Boolean,
                   visualizeSolution: Boolean,
                   outputPath: String) {

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

  def compareGnnSolvers(inputShapes: List[(Int, Int, Int, Int)],
                        inputGenerationStrategies: List[DataGenerationStrategy],
                        queryGenerationStrategies: List[DataGenerationStrategy],
                        solvers: List[(GNNSolver, String)]): Unit = {

    val runtime = Runtime.getRuntime
//    val NUM_PARTITIONS = 16
    val TIMEOUT = 250000

    new File(outputPath).mkdirs()
    val headers = Seq(
      "id",
      "name",
      "n",
      "q",
      "range",
      "data_bounds",
      "query_bounds",
      "intersection_area",
      "points_around_solution",
      "input_generation_strategy",
      "query_generation_strategy",
      "setup",
      "finding_real_sol",
      "after_pruning",
      "pruning_percentage",
      "total_time"
    ) ++
      (1 until 20).map(i => s"level_${i}_time") ++
      (1 until 20).map(i => s"level_${i}_querybounds") ++
      (1 until 20).map(i => s"level_${i}_pruning")

    val defLog = headers.map(_ -> "0").toMap

    val resultsCsv = CSVDictWriter(outputPath + "results.csv", headers)
    resultsCsv.writeHeader()

    for ((inputSize, querySize, range, iterationsCount) <- inputShapes) {
      val geometryFactory = new GeometryFactory()

      printf("Starting a new test with:\ndataSize: %d\n" +
               "querySize: %d\nrange: %d\n",
             inputSize,
             querySize,
             range)

      val operationId = Random.nextLong()

      val directoryName = outputPath +
        "data_size_" + inputSize +
        "_query_size_" + querySize +
        "_range_" + range +
        "_" + operationId + "/"

      new File(directoryName).mkdirs()
      val resultsFile = new PrintWriter(directoryName + "results.txt")
      val resultsStr = new StringBuilder()

      val solverAvgTimePerIteration = Array.fill(solvers.size)(0.0)
      val solverTimeOuts = Array.fill(solvers.size)(0)

      for {
        iter <- 1 to iterationsCount
        queryGenerationStrategy <- queryGenerationStrategies
        inputGenerationStrategy <- inputGenerationStrategies
        id = Random.nextInt(Int.MaxValue)
        fileBaseName = directoryName + "/" + id + "_"
      } {
        val solverResults: Array[(Point, Double)] =
          Array.fill(solvers.size)((null, -1))
        resultsStr.append("Iteration " + id + "\n")

        val dataSpatialRDD = {
          val rdd =
            inputGenerationStrategy.generate(querySize, range, sparkContext)
          rdd.analyze()
//          rdd.saveAsGeoJSON(fileBaseName + "query.geojson")
          rdd
        }

        val querySpatialRDD = {
          val rdd =
            queryGenerationStrategy.generate(inputSize, range, sparkContext)
          rdd.analyze()
//          rdd.saveAsGeoJSON(fileBaseName + "data.geojson")
          rdd
        }

        for (solverInd <- solvers.indices) {
          val (solver, solverName) = solvers(solverInd)

//          val querySpatialRDD = {
//            val location = fileBaseName + "query.geojson"
//            val splitter = FileDataSplitter.GEOJSON
//            val offset = 0
//            new PointRDD(sparkContext,
//                         location,
//                         offset,
//                         splitter,
//                         true,
//                         NUM_PARTITIONS,
//                         StorageLevel.MEMORY_ONLY)
//          }
//
//          val dataSpatialRDD = {
//            val location = fileBaseName + "data.geojson"
//            val splitter = FileDataSplitter.GEOJSON
//            val offset = 0
//            new PointRDD(sparkContext,
//                         location,
//                         offset,
//                         splitter,
//                         true,
//                         NUM_PARTITIONS,
//                         StorageLevel.MEMORY_ONLY)
//          }

          // invoke garbage collector
          runtime.gc()

          println(s"Starting $iter/$iterationsCount $solverName ${queryGenerationStrategy.getClass.getSimpleName}")
          val t0 = System.currentTimeMillis()
          val (logs, res, timeElapsed) = runWithTimeout(TIMEOUT) {
            val res = solver.solve(geometryFactory,
                                   dataSpatialRDD,
                                   querySpatialRDD,
                                   resultsStr,
                                   visualizeSolution,
                                   fileBaseName + solverName + "_")
            val timeElapsed = System.currentTimeMillis() - t0
            solverAvgTimePerIteration(solverInd) += timeElapsed
            resultsStr.append(
              solverName + " finished in " + timeElapsed + "ms\n")
            (res._1, res._2, timeElapsed)
          }.getOrElse({
            solverTimeOuts(solverInd) += 1
            resultsStr.append(solverName + " timed out\n")
            (Map(), (null, -1.0), -1)
          })

          val points_around_results =
            if (res._1 == null) {
              -1
            } else {
              RangeQuery
                .SpatialRangeQuery(dataSpatialRDD,
                                   new Circle(res._1, range / 10.0),
                                   false,
                                   false)
                .count()
            }

          solverResults(solverInd) = res
          resultsCsv.write(
            defLog ++ Map(
              "id" -> id.toString,
              "name" -> solverName,
              "total_time" -> timeElapsed.toString,
              "n" -> inputSize.toString,
              "q" -> querySize.toString,
              "range" -> range.toString,
              "data_bounds" -> dataSpatialRDD.boundaryEnvelope.toString,
              "query_bounds" -> querySpatialRDD.boundaryEnvelope.toString,
              "points_around_solution" -> points_around_results.toString,
              "intersection_area" ->
                dataSpatialRDD.boundaryEnvelope
                  .intersection(querySpatialRDD.boundaryEnvelope)
                  .getArea
                  .toString,
              "input_generation_strategy" -> inputGenerationStrategy.getClass.getSimpleName,
              "query_generation_strategy" -> queryGenerationStrategy.getClass.getSimpleName
            ) ++ logs)
          resultsCsv.flush()

          if (visualizeResult && res._1 != null) {
            println("Drawing the " + solverName + " Image")
            Visualization.buildScatterPlotWithResult(
              sparkContext,
              List(querySpatialRDD, dataSpatialRDD),
              res._1,
              fileBaseName +
                solverName + "_result")
          }

        }

        resultsStr.append("Results:\n")
        resultsStr.append(
          solvers.map(_._2).zip(solverResults).mkString("\n")
            + "\n\n")

        sparkContext.getPersistentRDDs.foreach {
          case (_, rdd) => rdd.unpersist()
        }

        resultsFile.append(resultsStr.toString())
        resultsStr.clear()

      }

      resultsStr.append("\nFor inputSize: " + inputSize + "\n")
      resultsStr.append(
        "\nFor querySize: " + querySize + "\nRange: " +
          range + "\n")

      resultsStr.append("Using Timeout: " + TIMEOUT + "ms\n")
      resultsStr.append("Iterations Count: " + iterationsCount + "\n")

      for (solverInd <- solvers.indices) {
        if (solverTimeOuts(solverInd) < iterationsCount * queryGenerationStrategies.size * inputGenerationStrategies.size) {
          resultsStr.append(
            "Avg " + solvers(solverInd)._2 + " Time per " +
              "Iteration: " +
              solverAvgTimePerIteration(solverInd) /
                (iterationsCount * queryGenerationStrategies.size * inputGenerationStrategies.size - solverTimeOuts(
                  solverInd)) + "ms\n")
        }
      }

      for (solverInd <- solvers.indices) {
        resultsStr.append(
          solvers(solverInd)._2 + " Timeouts: " +
            solverTimeOuts(solverInd) + "\n")
      }
      println(resultsStr.toString())

      resultsFile.append(resultsStr.toString())
      resultsFile.close()
    }

    resultsCsv.close()
  }
}

object Benchmark {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("GeoSparkRunnableExample")
      .setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator",
             classOf[GeoSparkVizKryoRegistrator].getName)

    val sparkContext = new SparkContext(conf)
    val runId = System.currentTimeMillis()
//    val runId = "benchmark_qtree_rtree_200k_300k_500k_1m"

    val benchmark = new GNNBenchmark(
      sparkContext,
      visualizeSolution = false,
      visualizeResult = true,
      outputPath = System.getProperty("user.dir") +
        "/target/gnn/" + runId + "/"
    )

    benchmark.compareGnnSolvers(
      List(
//        (10000, 10000, 800000, 200),
//        (50000, 50000, 800000, 100),
//        (100000, 100000, 800000, 40),
//        (200000, 200000, 800000, 30),
//        (10000, 10000, 800000, 30),
//          (20000, 20000, 800000, 30),
        (100000, 100000, 800000, 10),
        (200000, 200000, 800000, 10),
          (300000, 300000, 800000, 10),
          (500000, 500000, 800000, 10)
      ),
      inputGenerationStrategies = List(GenerateUniformData()),
      queryGenerationStrategies = List(
        GenerateUniformData(),
        GenerateGuassianData(),
        GenerateNonUniformData(),
        GenerateExponentialData(),
//        GenerateZipfData(.8),
        GenerateZipfData(.5)
//        GenerateZipfData(.3)
      ),
      solvers = List(
        //                      (GNNApprox, "ApproxGNN"),
//                  (NaiveGNN, "GNN_Naive"),
//        (GNNWithPruning, "GNN_Pruning")
          (GNNWithPruning(GridType.QUADTREE, IndexType.QUADTREE), "GNN_Pruning_quadtree"),
            (GNNWithPruning(GridType.RTREE, IndexType.RTREE), "GNN_Pruning_rtree")
      )
    )

    sparkContext.stop()

  }

}
