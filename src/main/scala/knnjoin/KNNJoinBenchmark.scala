package knnjoin


import java.io.{File, PrintWriter}

import com.vividsolutions.jts.geom._
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.spatialRDD.{LineStringRDD, PointRDD, PolygonRDD, SpatialRDD}
import utils.{GenerateUniformData, SparkRunner, Visualization}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random


/**
  * The Class ScalaExample.
  */
class KNNJoinBenchmark(sparkContext: SparkContext,
                       geometryFactory: GeometryFactory,
                       visualize: Boolean,
                       outputPath: String) {


  def runWithTimeout[T](timeoutMs: Long)(f: => T): Option[T] = {
    try {
      Some(Await.result(Future(f), timeoutMs milliseconds))
    } catch {
      case _: TimeoutException => None
      case e: Exception => {
        println(e.getMessage)
        e.printStackTrace()
        None
      }
    }
  }

  def compareKNNJoinSolvers(solvers: List[(KNNJoinSolver, String)],
                            inputSize: Int, inputRange: Int,
                            querySize: Int, queryRange: Int,
                            k: Int): PrintWriter = {


    printf("Starting a new test with:\ndataSize: %d, dataRange: %d\n" +
      "querySize: %d, queryRange: %d\nK: %d\n",
      inputSize, inputRange, querySize, queryRange, k)

    val iterationsCount = 3
    val timeout = 200000
    val operationId = Random.nextLong()

    val directoryName = outputPath +
      "data_size_" + inputSize +
      "_data_range_" + inputRange +
      "_query_size_" + querySize +
      "_query_range_" + queryRange +
      "_k_" + k +
      "_" + operationId + "/"

    new File(directoryName).mkdirs()

    val resultsStr = new StringBuilder()

    val solverAvgTimePerIteration = Array.fill(solvers.size)(0.0)
    val solverTimeOuts = Array.fill(solvers.size)(0)

    for (iteration <- 1 to iterationsCount) {
      val fileBaseName = directoryName + "/" + iteration + "_"
      resultsStr.append("Iteration " + iteration + "\n")

      val dataSpatialRDD = {
        val rdd = GenerateUniformData().generate(sparkContext, querySize,
          queryRange)
        rdd.analyze()
        rdd
      }

      val querySpatialRDD = {
        val rdd = GenerateUniformData().generate(sparkContext, inputSize, inputRange)
        rdd.analyze()
        rdd
      }

      var resultList: List[JavaPairRDD[Point, java.util.List[Point]]] = List()
      for (solverInd <- solvers.indices) {
        val (solver, solverName) = solvers(solverInd)

        val t0 = System.nanoTime()
        val res = runWithTimeout(timeout) {
          val res = solver.solve(geometryFactory, dataSpatialRDD, queryRDD = querySpatialRDD, k,
            resultStr = resultsStr, visualize = visualize, outputPath = fileBaseName + solverName + "_")
          // Trying to defeat the lazy computations
          res.count()

          val timeElapsed = (System.nanoTime() - t0) / 1000000.0
          solverAvgTimePerIteration(solverInd) += timeElapsed
          println(solverName + " finished in " + timeElapsed + "ms\n")

          resultsStr.append(solverName + " finished in " + timeElapsed + "ms\n")
          resultList ::= res
          res
        }.getOrElse({
          solverTimeOuts(solverInd) += 1
          println(solverName + " timed out\n")
          resultsStr.append(solverName + " timed out\n")
          null
        })

//        solverResults(solverInd) = res.collect().toList

        if (visualize /*&& res != null*/) {
          println("Drawing the " + solverName + " Image")

          val geometryFactory = new GeometryFactory()
          val linesRDD = new LineStringRDD(
            res.rdd.flatMap({
              case (center, points) =>
                points.toList.map(
                  p => geometryFactory.createLineString(Array(center.getCoordinate, p.getCoordinate))
                )
            })
          )
          linesRDD.analyze()

          val polygonBounds = dataSpatialRDD
            .getPartitioner.getGrids.map(
            env => geometryFactory.createPolygon(Array(
              new Coordinate(env.getMinX, env.getMinY),
              new Coordinate(env.getMinX, env.getMaxY),
              new Coordinate(env.getMaxX, env.getMaxY),
              new Coordinate(env.getMaxX, env.getMinY),
              new Coordinate(env.getMinX, env.getMinY)
            )))

          val polygonRDD = sparkContext.parallelize(polygonBounds)
          val boundsRDD = new PolygonRDD(polygonRDD)
          boundsRDD.analyze()

          Visualization.buildScatterPlot(List(boundsRDD, linesRDD, dataSpatialRDD, querySpatialRDD), fileBaseName +
            solverName + "_result")
          //          val missingPointsRDD = new PointRDD(
          //            sparkContext.parallelize(
          //              querySpatialRDD.rawSpatialRDD.collect().filterNot(res.keys().collect().toSet)
          //            )
          //          )
          //          missingPointsRDD.analyze()

          //          Visualization.buildScatterPlot(List(boundsRDD, missingPointsRDD), fileBaseName +
          //            solverName + "_result")
        }

      }

//      println(ResultChecker.compare(resultList(0), resultList(1), k));
      //          println(res.count(), res.countByKey().size)
      //      for {
      //        x <- solverResults
      //        y <- solverResults
      //      } {
      //        if (x.size > y.size) {
      //          println("COMPARE")
      //          println(x.filterNot(y.toSet).mkString(", "))
      //        } else if (x.size < y.size) {
      //          println("COMPARE")
      //          println(y.filterNot(x.toSet).mkString(", "))
      //        }
      //      }
      //      resultsStr.append("Results:\n")
      //      resultsStr.append(solvers.map(_._2).zip(solverResults).mkString("\n")
      //        + "\n\n")

      sparkContext.getPersistentRDDs.foreach { case (_, rdd) => rdd.unpersist() }

    }

    resultsStr.append("\nFor inputSize: " + inputSize + ", inputRange: " +
      inputRange + "\n")
    resultsStr.append("\nFor querySize: " + querySize + ", queryRange: " +
      queryRange + "\n")
    resultsStr.append("\nFor k: " + k + "\n\n")

    resultsStr.append("Using Timeout: " + timeout + "ms\n")
    resultsStr.append("Iterations Count: " + iterationsCount + "\n")

    for (solverInd <- solvers.indices) {
      if (solverTimeOuts(solverInd) < iterationsCount) {
        resultsStr.append("Avg " + solvers(solverInd)._2 + " Time per " +
          "Iteration: " +
          solverAvgTimePerIteration(solverInd) /
            (iterationsCount - solverTimeOuts(solverInd)) + "ms\n"
        )
      }
    }

    for (solverInd <- solvers.indices) {
      resultsStr.append(solvers(solverInd)._2 + " Timeouts: " +
        solverTimeOuts(solverInd) + "\n")
    }
    println(resultsStr.toString())

    new PrintWriter(directoryName + "/results.txt") {
      write(resultsStr.toString())
      close()
    }
  }
}

object KNNJoinBenchmark {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkRunner.start()
    val geometryFactory = new GeometryFactory()
    val runId = System.currentTimeMillis()

    for (((querySize, queryRange), (inputSize, inputRange), k) <- List(
      ((10000, 100000),
        (10000, 100000),
        500),
      ((50000, 100000),
        (50000, 100000),
        100),
      ((100000, 100000),
        (100000, 100000),
        20),
      ((200000, 100000),
        (200000, 100000),
        10)
    )) {
      val benchmark = new KNNJoinBenchmark(
        sparkContext,
        geometryFactory,
        visualize = false,
        outputPath = System.getProperty("user.dir") +
          "/target/knnjoin/" + runId + "/"
      )
      benchmark.compareKNNJoinSolvers(
        List(
          (new KNNJoinInPartitionOnly(), "KNN_InPartitionOnly"),
          (new KNNJoinWithCirclesWithReduceByKey(), "KNN_WithCirclesWithReduceByKey"),
          (new KNNJoinWithCircles(), "KNN_WithCirclesWithGroupByKey")
//       (KNNJoinNaive, "KNN_Naive")
        ), inputSize, inputRange, querySize, queryRange, k)

    }
    sparkContext.stop()

  }
}


