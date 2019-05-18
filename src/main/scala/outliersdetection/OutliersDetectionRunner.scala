
package outliersdetection

import java.io.File

import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.KNNQuery
import utils.{GenerateExponentialData, GenerateGaussianData, GenerateNonUniformData, GenerateUniformData, SparkRunner}

import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Random

object OutliersDetectionRunner {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sc = SparkRunner.start()
    deleteOldValidation()
    for (_ <- 0 until 100) {
      val data = GenerateUniformData().generate(sc, 10000, 800000)

      val n = 50
      val k = 50

      data.analyze
      var nextRdd = data
      var prevCount = 0L
      var nextCount = 0L
      var pruningIteration = 1

      Array(
//        new ExpanderByTotalPointsRatio(0.2, 5000)
//        new ExpanderWithAreaBounds(0.2, 30000, 1.0 / 300, 1.0 / 5000, x => x.getBounds.getArea)
      new ExpanderWithAreaBounds(0.2, 30000, 1.0 / 300, 1.0 / 5000, x => x.getPointsCount * 1.0 / x.getBounds.getArea)
//        new ExpanderByPointsRatioPerGrid(0.2, 30000, x => x.getPointsCount * 1.0 / x.getBounds.getArea)
      ).foreach(expander => {
        println(expander.getClass.getName)
        for (_ <- 0 to 1) {
          prevCount = nextRdd.countWithoutDuplicates
          nextRdd = OutliersDetectionGeneric(GridType.QUADTREE, IndexType.QUADTREE, expander).findOutliers(nextRdd, n, k, s"visualization/${expander.getClass.getName}/$pruningIteration")._2
          nextCount = nextRdd.countWithoutDuplicates
          println("Pruning = " + ((1.0 * data.countWithoutDuplicates - nextCount) / data.countWithoutDuplicates * 100.0) + "\n")

          pruningIteration += 1
        }
        if (nextRdd.rawSpatialRDD.count() < data.countWithoutDuplicates()) {

          data.spatialPartitioning(GridType.RTREE)
          data.buildIndex(IndexType.RTREE, true)
          data.buildIndex(IndexType.RTREE, false)

          val possibleAns = nextRdd.rawSpatialRDD.collect().toList
          val ans = OutliersDetectionNaiveWithKNNJoin.findOutliersNaive(data, k, n)
          println(s"Finished Naiive Execution and found ${ans.size} outliers")
          Plotter.visualizeNaiive(sc, data.boundaryEnvelope, ans, "natiive")


          println(if (possibleAns.containsAll(ans)) {
            "VALID"
          }
          else {
            data.saveAsGeoJSON(s"invalid_pruning/${Random.nextLong()}_data_geojson")
            "INVALID"

          })

          val customAns = ans.map(p => {
            KNNQuery.SpatialKnnQuery(data, p, k, true).map(p2 => p.distance(p2)).max
          })
          val customPossibleAns = OutliersDetectionNaiveWithKNNJoin.findOutliersNaive2(nextRdd, k, n, data).map(p => {
            KNNQuery.SpatialKnnQuery(data, p, k, true).map(p2 => p.distance(p2)).max
          })

          println(
            if (customAns.forall(x => customPossibleAns.exists(y => math.abs(x - y) < 1e-6))) {
              "VALID2"
            }
            else {
              "INVALID2" + ", \n" + customAns.sorted.mkString(",") + "\n" + customPossibleAns.sorted.mkString(",")
            }
          )
        }

      })
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
    val visualizationsFile = new File("visualization/outliers")
    if (visualizationsFile.exists()) {
      System.out.println("Delete old visualizations")
      val process = Runtime.getRuntime.exec(s"rm -rf ${visualizationsFile.getAbsolutePath}")
      process.waitFor
    }
  }
}