package outliersdetection

import java.io.File

import com.vividsolutions.jts.geom.Point
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import utils.GenerateGuassianData

import scala.collection.JavaConversions._
import scala.language.postfixOps

object Example {

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
    data.analyze
    data.spatialPartitioning(GridType.RTREE)
    data.buildIndex(IndexType.QUADTREE, true)
    data.indexedRDD.cache
    var nextRdd = data
    var prevCount = 0L
    var nextCount = 0L
    var pruningIteration = 1
    do {
      prevCount = nextRdd.countWithoutDuplicates
      System.out.println("Before # of Points = " + prevCount)
      nextRdd = OutliersDetectionQ.findOutliers(nextRdd, 300, 300, pruningIteration, 4000)
      nextCount = nextRdd.countWithoutDuplicates
      System.out.println("After # of Points = " + nextCount + "\n")
      System.out.println("Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")
      pruningIteration += 1
    } while ( {
      nextCount < prevCount
    })
    System.out.println("Pruning Iterations = " + pruningIteration)
    runNiiveSolution(sc, nextRdd)
    //        compareToNaiiveSolution(sc, data, nextRdd);
    sc.stop
  }

  private def runNiiveSolution(sc: JavaSparkContext, data: PointRDD): Unit = {
    data.spatialPartitioning(GridType.RTREE)
    data.buildIndex(IndexType.QUADTREE, true)
    data.indexedRDD.cache
    val ans = OutliersDetectionNaiive.findOutliersNaive(data, 300, 300)
    val solutionRdd = new PointRDD(sc.parallelize(ans))
    solutionRdd.analyze
    Plotter.visualizeNaiive(sc, solutionRdd, "NaiiveSolution")
    System.out.println(ans.size + " Outliers were found!")
  }

  private def compareToNaiiveSolution(sc: JavaSparkContext, data: PointRDD, nextRdd: PointRDD): Unit = {
    data.spatialPartitioning(GridType.RTREE)
    data.buildIndex(IndexType.QUADTREE, true)
    data.indexedRDD.cache
    var found: Int = 0
    val doubtList = nextRdd.spatialPartitionedRDD.collect()
    val ans: List[Point] = OutliersDetectionNaiive.findOutliersNaive(data, 100, 100)
    for (p <- ans) {
      for (x <- doubtList) {
        if ((x.getX == p.getX) && (x.getY == p.getY)) found += 1
      }
    }
    System.out.println(if (ans.size == found) "VALID SOLUTION"
    else "INVALID SOLUTION")
    val solutionRdd = new PointRDD(sc.parallelize(ans))
    solutionRdd.analyze
    Plotter.visualizeNaiive(sc, solutionRdd, "NaiiveSolution")
    System.out.println(ans.size + " Outliers were found!")
  }

  private def deleteOldValidation() = {
    System.out.println("Delete old visualizations")
    val visualizationsFile = new File("visualization/outliers")
    val process = Runtime.getRuntime.exec(String.format("rm -rf %s", visualizationsFile.getAbsolutePath))
    process.waitFor
  }
}