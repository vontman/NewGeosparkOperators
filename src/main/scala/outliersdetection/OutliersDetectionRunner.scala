package outliersdetection

import org.datasyslab.geospark.enums.{GridType, IndexType}
import utils.{GenerateRandomGaussianClusters, SparkRunner}

import scala.language.postfixOps

object OutliersDetectionRunner {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val sc = SparkRunner.start()

    val data = GenerateRandomGaussianClusters(2, 500, 800).generate(sc, 20000, 10000000, numPartitions = 4)

    val n = 100
    val k = 100

    data.analyze
    val originalBounds = data.boundaryEnvelope

    val expander = new ExpanderByPointsRatioPerGrid(0.1, 700000, x => x.getBounds.getArea)

    val (genericLogs, genericAns) = OutliersDetectionGeneric(GridType.KDBTREE, IndexType.QUADTREE, expander)
      .findOutliers(
        originalBounds,
        data,
        n,
        k,
        s"visualization/${System.currentTimeMillis}")

    val naiveAns = OutliersDetectionNaiveWithKNNJoin.findOutliersNaive(data, k, n)._2

    assert(naiveAns.forall(genericAns.contains) && genericAns.forall(naiveAns.contains))

    println("VALID :\")")

    sc.getPersistentRDDs.foreach(_._2.unpersist())
    sc.stop
  }

}
