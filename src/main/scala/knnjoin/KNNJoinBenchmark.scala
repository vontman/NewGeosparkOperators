//package knnjoin
//
//
//import java.io.{File, PrintWriter}
//
//import com.vividsolutions.jts.geom._
//import com.vividsolutions.jts.index.strtree.STRtree
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.serializer.KryoSerializer
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.{SparkConf, SparkContext}
//import org.datasyslab.geospark.enums.FileDataSplitter
//import org.datasyslab.geospark.spatialRDD.{LineStringRDD, PointRDD, PolygonRDD, SpatialRDD}
//import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
//import utils.{GenerateUniformData, InputGenerator, Visualization}
//
//import scala.collection.JavaConversions._
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent._
//import scala.concurrent.duration._
//import scala.util.Random
//
//
///**
//  * The Class ScalaExample.
//  */
//class KNNJoinBenchmark(sparkContext: SparkContext,
//                       geometryFactory: GeometryFactory,
//                       visualize: Boolean,
//                       outputPath: String) {
//
//
//  def runWithTimeout[T](timeoutMs: Long)(f: => T): Option[T] = {
//    try {
//      Some(Await.result(Future(f), timeoutMs milliseconds))
//    } catch {
//      case _: TimeoutException => None
//      case e: Exception => {
//        println(e.getMessage)
//        e.printStackTrace()
//        None
//      }
//    }
//  }
//
//  def compareKNNJoinSolvers(solvers: List[(KNNJoinSolver, String)],
//                            inputSize: Int, inputRange: Int,
//                            querySize: Int, queryRange: Int) = {
//
//    val runtime = Runtime.getRuntime
//    printf("Starting a new test with:\ndataSize: %d, dataRange: %d\n" +
//      "querySize: %d, queryRange: %d\n",
//      inputSize, inputRange, querySize, queryRange)
//    new File(outputPath).mkdirs()
//
//    val iterationsCount = 5
//    val timeout = 20000
//    val operationId = Random.nextLong()
//
//    val directoryName = outputPath +
//      "data_size_" + inputSize +
//      "_data_range_" + inputRange +
//      "_query_size_" + querySize +
//      "_query_range_" + queryRange +
//      "_" + operationId + "/"
//
//    new File(directoryName).mkdirs()
//
//    val resultsStr = new StringBuilder()
//
//    val solverAvgTimePerIteration = Array.fill(solvers.size)(0.0)
//    val solverTimeOuts = Array.fill(solvers.size)(0)
//
//
//    for (iteration <- 1 to iterationsCount) {
//      val solverResults: Array[List[(Point, Point)]] = Array.fill(solvers.size)(
//        null)
//      val fileBaseName = directoryName + "/" + iteration + "_"
//      resultsStr.append("Iteration " + iteration + "\n")
//
//      val querySpatialRDD = {
//        val rdd = InputGenerator.generateInputData(querySize,
//          queryRange, sparkContext)
//        rdd.analyze()
////        rdd.saveAsGeoJSON(fileBaseName + "query.geojson")
//        rdd
//      }
//
//      val dataSpatialRDD = {
//        val rdd = InputGenerator.generateInputData(inputSize, inputRange, sparkContext)
//        rdd.analyze()
////        rdd.saveAsGeoJSON(fileBaseName + "data.geojson")
//        rdd
//      }
//
//      for (solverInd <- solvers.indices) {
//        val (solver, solverName) = solvers(solverInd)
//
////        val dataSpatialRDD = {
////          val location = fileBaseName + "data.geojson"
////          val splitter = FileDataSplitter.GEOJSON
////          val numPartitions = 24
////          val offset = 0
////          val spatialRDD: SpatialRDD[Point] = new PointRDD(sparkContext, location,
////            offset,
////            splitter, true,
////            numPartitions,
////            StorageLevel.MEMORY_ONLY)
////
////          spatialRDD
////        }
////
////        val querySpatialRDD = {
////          val location = fileBaseName + "query.geojson"
////          val splitter = FileDataSplitter.GEOJSON
////          val numPartitions = 24
////          val offset = 0
////          val spatialRDD: SpatialRDD[Point] = new PointRDD(sparkContext, location,
////            offset,
////            splitter, true,
////            numPartitions,
////            StorageLevel.MEMORY_ONLY)
////
////          spatialRDD
////        }
//
//        // invoke garbage collector
//        runtime.gc()
//        val t0 = System.nanoTime()
//        val res = runWithTimeout(timeout) {
//          val res = solver.solve(geometryFactory, dataSpatialRDD, queryRDD = querySpatialRDD, k = 5,
//            resultStr = resultsStr, visualize = visualize, outputPath = fileBaseName + solverName + "_")
//          val timeElapsed = (System.nanoTime() - t0) / 1000000.0
//          solverAvgTimePerIteration(solverInd) += timeElapsed
//          resultsStr.append(solverName + " finished in " + timeElapsed + "ms\n")
//          res
//        }.getOrElse({
//          solverTimeOuts(solverInd) += 1
//          resultsStr.append(solverName + " timed out\n")
//          null
//        })
//
////        solverResults(solverInd) = res.collect().toList
//
//        if (visualize && res != null) {
//          println("Drawing the " + solverName + " Image")
//
//          val geometryFactory = new GeometryFactory()
//          val linesRDD = new LineStringRDD(
//            res.rdd.map(
//              p => geometryFactory.createLineString(Array(p._1.getCoordinate, p._2.getCoordinate))
//            )
//          )
//          linesRDD.analyze()
//
////          val boundsRDD = new PolygonRDD(
////            dataSpatialRDD
////              .indexedRDD
////              .rdd
////              .map(index => index.asInstanceOf[STRtree].getRoot.getBounds
////                .asInstanceOf[Envelope])
////              .map(
////                env => geometryFactory.createPolygon(Array(
////                  new Coordinate(env.getMinX, env.getMinY),
////                  new Coordinate(env.getMinX, env.getMaxY),
////                  new Coordinate(env.getMaxX, env.getMaxY),
////                  new Coordinate(env.getMaxX, env.getMinY),
////                  new Coordinate(env.getMinX, env.getMinY)
////                ))
////              ))
////          boundsRDD.analyze()
//          val polygonBounds = dataSpatialRDD
//            .getPartitioner.getGrids.map(
//            env => geometryFactory.createPolygon(Array(
//              new Coordinate(env.getMinX, env.getMinY),
//              new Coordinate(env.getMinX, env.getMaxY),
//              new Coordinate(env.getMaxX, env.getMaxY),
//              new Coordinate(env.getMaxX, env.getMinY),
//              new Coordinate(env.getMinX, env.getMinY)
//            )))
//
//          val polygonRDD = sparkContext.parallelize(polygonBounds)
//          val boundsRDD = new PolygonRDD(polygonRDD)
//          boundsRDD.analyze()
//
//          Visualization.buildScatterPlot(List(boundsRDD, linesRDD, dataSpatialRDD, querySpatialRDD), fileBaseName +
//            solverName + "_result")
//          //          val missingPointsRDD = new PointRDD(
//          //            sparkContext.parallelize(
//          //              querySpatialRDD.rawSpatialRDD.collect().filterNot(res.keys().collect().toSet)
//          //            )
//          //          )
//          //          missingPointsRDD.analyze()
//
//          //          Visualization.buildScatterPlot(List(boundsRDD, missingPointsRDD), fileBaseName +
//          //            solverName + "_result")
//        }
//
//      }
//      //          println(res.count(), res.countByKey().size)
//      //      for {
//      //        x <- solverResults
//      //        y <- solverResults
//      //      } {
//      //        if (x.size > y.size) {
//      //          println("COMPARE")
//      //          println(x.filterNot(y.toSet).mkString(", "))
//      //        } else if (x.size < y.size) {
//      //          println("COMPARE")
//      //          println(y.filterNot(x.toSet).mkString(", "))
//      //        }
//      //      }
//      //      resultsStr.append("Results:\n")
//      //      resultsStr.append(solvers.map(_._2).zip(solverResults).mkString("\n")
//      //        + "\n\n")
//
//      sparkContext.getPersistentRDDs.foreach { case (_, rdd) => rdd.unpersist() }
//
//    }
//
//    resultsStr.append("\nFor inputSize: " + inputSize + ", inputRange: " +
//      inputRange + "\n")
//    resultsStr.append("\nFor querySize: " + querySize + ", queryRange: " +
//      queryRange + "\n")
//
//    resultsStr.append("Using Timeout: " + timeout + "ms\n")
//    resultsStr.append("Iterations Count: " + iterationsCount + "\n")
//
//    for (solverInd <- solvers.indices) {
//      if (solverTimeOuts(solverInd) < iterationsCount) {
//        resultsStr.append("Avg " + solvers(solverInd)._2 + " Time per " +
//          "Iteration: " +
//          solverAvgTimePerIteration(solverInd) /
//            (iterationsCount - solverTimeOuts(solverInd)) + "ms\n"
//        )
//      }
//    }
//
//    for (solverInd <- solvers.indices) {
//      resultsStr.append(solvers(solverInd)._2 + " Timeouts: " +
//        solverTimeOuts(solverInd) + "\n")
//    }
//    println(resultsStr.toString())
//
//    new PrintWriter(directoryName + "/results.txt") {
//      write(resultsStr.toString())
//      close()
//    }
//  }
//}
//
//object KNNJoinBenchmark {
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("akka").setLevel(Level.ERROR)
//    val conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]")
//    conf.set("spark.serializer", classOf[KryoSerializer].getName)
//    conf.set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
//
//    val sparkContext = new SparkContext(conf)
//    val geometryFactory = new GeometryFactory()
//    val runId = System.currentTimeMillis()
//
//    for (((querySize, queryRange), (inputSize, inputRange)) <- List(
////      ((500, 10000),
////        (5000, 10000)),
////      ((1000, 10000),
////        (10000, 10000)),
////      ((10000, 10000),
////        (10000, 10000)),
////      ((50000, 50000),
////        (50000, 50000)),
////      ((100000, 100000),
////        (100000, 100000))
////      ((1000000, 200000),
////        (1000000, 200000)),
//      ((100000, 300000),
//        (100000, 300000))
//
//    )) {
//      val benchmark = new KNNJoinBenchmark(
//        sparkContext,
//        geometryFactory,
//        visualize = false,
//        outputPath = System.getProperty("user.dir") +
//          "/target/knnjoin/" + runId + "/"
//      )
//      benchmark.compareKNNJoinSolvers(
//        List(
//          (new KNNJoinWithCircles(), "KNN_WithCircles"),
//          (new KNNJoinInPartitionOnly(), "KNN_InPartitionOnly"),
//            (KNNJoinWithLocationSpark, "KNN_location_spark")
////          (KNNJoinNaive, "KNN_Naive")
//        ), inputSize, inputRange, querySize, queryRange)
//
//    }
//    sparkContext.stop()
//
//  }
//
//
//}
//
//
