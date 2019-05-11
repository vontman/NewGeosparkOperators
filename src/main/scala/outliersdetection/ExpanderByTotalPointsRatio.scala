package outliersdetection

import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

object ExpanderByTotalPointsRatio {
  def getPermutations: List[(LevelExpander, String)] = {

    for {
      maxPartitionsRatio <- List(.1)
      threshold <- List(30000, 15000, 10000, 5000)

    } yield (
      new ExpanderByTotalPointsRatio(maxPartitionsRatio, threshold), s"ExpanderByTotalPointsRatio_${maxPartitionsRatio}_${threshold}"
    )

  }
}

class ExpanderByTotalPointsRatio(
    partitionsToPointsRatio: Double,
    maxThreshold: Int
) extends LevelExpander {

  private def levelsExpander(rdd: PointRDD): RDD[IndexNode] = {
    val numberOfPartitions = math.min(
      rdd.rawSpatialRDD.count() * partitionsToPointsRatio,
      maxThreshold)

    var nextLevelPartitions = rdd.indexedRDD.rdd
      .map({
        case t: Quadtree => IndexNode(t.getRoot)
        case t: STRtree  => IndexNode(t.getRoot)
      })
      .filter(_.getPointsCount > 0)

    var curCount = nextLevelPartitions.count()
    var selectedLevel = nextLevelPartitions

    while (curCount < numberOfPartitions) {
      nextLevelPartitions = nextLevelPartitions
        .mapPartitions(_.flatMap(index => index.getChildren))
        .cache()
      curCount = nextLevelPartitions.count()
      if (curCount < numberOfPartitions)
        selectedLevel = nextLevelPartitions
    }

    selectedLevel.cache()
  }

  override def expand(inputRDD: PointRDD): RDD[IndexNode] =
    levelsExpander(inputRDD)
}
