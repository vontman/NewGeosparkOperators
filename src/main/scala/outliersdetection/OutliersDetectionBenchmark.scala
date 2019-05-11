package outliersdetection

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import utils.{GenerateExponentialData, GenerateGuassianData, GenerateZipfData}

import scala.language.postfixOps

class OutliersDetectionBenchmark(sparkContext: SparkContext,
                                 visualizeResult: Boolean,
                                 visualizeSolution: Boolean,
                                 outputPath: String) {

}

object OutliersDetectionRunner {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
    val sc = new JavaSparkContext(conf)

    deleteOldValidation()
    val data = GenerateGuassianData().generate(101000, 800000, sc.sc)


    //(data, n, k)
    val input: List[(PointRDD, Int, Int)] = List(
      (GenerateGuassianData().generate(101000, 1e6.asInstanceOf[Int], sc.sc), 300, 300),
      (GenerateZipfData(0.5).generate(101000, 1e6.asInstanceOf[Int], sc.sc), 300, 300),
      (GenerateExponentialData().generate(101000, 1e6.asInstanceOf[Int], sc.sc), 300, 300),
    )


    val n = 300
    val k = 300
    input.foreach(input => {
      val data = input._1
      data.analyze
      var nextRdd = data
      var prevCount = 0L
      var nextCount = 0L
      var pruningIteration = 1

      for (_ <- 0 to 1) {
        prevCount = nextRdd.countWithoutDuplicates
        nextRdd = OutliersDetectionGeneric(GridType.QUADTREE, IndexType.QUADTREE, new ExpanderByPointsRatioPerGrid(0.1, node => {
          node.getBounds.getArea
        }))
          .findOutliers(nextRdd, input._2, input._3, pruningIteration)
        nextCount = nextRdd.countWithoutDuplicates
        println("Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")

        pruningIteration += 1
      }
    })

    sc.stop
  }

  private def deleteOldValidation() = {
    System.out.println("Delete old visualizations")
    val visualizationsFile = new File("visualization/outliers")
    val process = Runtime.getRuntime.exec(String.format("rm -rf %s", visualizationsFile.getAbsolutePath))
    process.waitFor
  }
}