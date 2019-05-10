package outliersdetection

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import utils.GenerateGuassianData

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
    val data = GenerateGuassianData().generate(101000, 800000, sc.sc)

    val n = 300
    val k = 300

    data.analyze
    var nextRdd = data
    var prevCount = 0L
    var nextCount = 0L
    var pruningIteration = 1
    for (_ <- 0 to 1) {
      prevCount = nextRdd.countWithoutDuplicates
      nextRdd = OutliersDetectionGeneric(GridType.QUADTREE, IndexType.QUADTREE, 4000).findOutliers(nextRdd, n, k, pruningIteration)
      nextCount = nextRdd.countWithoutDuplicates
      println("Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")

      pruningIteration += 1
    }

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
    System.out.println("Delete old visualizations")
    val visualizationsFile = new File("visualization/outliers")
    val process = Runtime.getRuntime.exec(String.format("rm -rf %s", visualizationsFile.getAbsolutePath))
    process.waitFor
  }
}