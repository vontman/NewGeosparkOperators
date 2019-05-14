
package outliersdetection

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import utils.GenerateUniformData

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionRunner {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
    val sc = new JavaSparkContext(conf)
    deleteOldValidation()
    val data = GenerateUniformData().generate(sc, 10000, 800000)

    val n = 100
    val k = 100

    data.analyze
    var nextRdd = data
    var prevCount = 0L
    var nextCount = 0L
    var pruningIteration = 1

    data.spatialPartitioning(GridType.RTREE)
    data.buildIndex(IndexType.RTREE, true)
    val ans = OutliersDetectionNaiive.findOutliersNaive(data, k, n)
    println(s"Finished Naiive Execution and found ${ans.size} outliers")
    Plotter.visualizeNaiive(sc, data.boundaryEnvelope, ans, "natiive")


    Array(
      //      new ExpanderByTotalPointsRatio(0.1, 5000),
      new ExpanderWithAreaBounds(0.1, 30000, 1.0 / 300, 1.0 / 5000, _.getBounds.getArea),
      new ExpanderByPointsRatioPerGrid(0.1, 30000, x => x.getPointsCount * 1.0 / x.getBounds.getArea)
    ).foreach(expander => {
      for (_ <- 0 to 1) {
        prevCount = nextRdd.countWithoutDuplicates
        nextRdd = OutliersDetectionGeneric(GridType.QUADTREE, IndexType.QUADTREE, expander).findOutliers(nextRdd, n, k, s"visualization/${expander.getClass.getName}/$pruningIteration")._2
        nextCount = nextRdd.countWithoutDuplicates
        println("Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")

        pruningIteration += 1
      }


      val possibleAns = nextRdd.rawSpatialRDD.collect().toList

      println(if (possibleAns.containsAll(ans)) {
        "VALID"
      }
      else {
        "INVALID"
      })
    })

    //    val ans =
    //      new PointRDD(sc.parallelize(new KNNJoinWithCircles().solve(nextRdd, nextRdd, k)
    //        .rdd.sortBy(p => p._1.disjoint(p._2), ascending = false)
    //        .take(n).map(_._1).toList))
    //
    //    ans.analyze()
    //    ans.spatialPartitioning(GridType.QUADTREE)
    //    ans.buildIndex(IndexType.QUADTREE, true)
    //    Plotter.visualize(sc, ans, "solution", data.boundaryEnvelope)

    sc.stop
  }

  private def deleteOldValidation() = {
    val visualizationsFile = new File("visualization/outliers")
    if (visualizationsFile.exists()) {
      System.out.println("Delete old visualizations")
      val process = Runtime.getRuntime.exec(s"rm -rf ${visualizationsFile.getAbsolutePath}")
      process.waitFor
    }
  }
}