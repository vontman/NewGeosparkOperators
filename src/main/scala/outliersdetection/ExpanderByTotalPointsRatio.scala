package outliersdetection

import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

class ExpanderByTotalPointsRatio(
                                  partitionsToPointsRatio: Double,
                                  maxThreshold: Int
                                ) extends LevelExpander {

  val parametersList = List(
    (),
    (),
    ()
  )

  private def levelsExpander(rdd: PointRDD): RDD[IndexNode] = {
    val numberOfPartitions = math.min(rdd.countWithoutDuplicates() * partitionsToPointsRatio, maxThreshold)

    var nextLevelPartitions = rdd.indexedRDD.rdd
      .map({
        case t: Quadtree => IndexNode(t.getRoot)
        case t: STRtree => IndexNode(t.getRoot)
      })
      .filter(_.getPointsCount > 0)

    var curCount = nextLevelPartitions.count()

    while (curCount < numberOfPartitions) {
      nextLevelPartitions = nextLevelPartitions
        .flatMap(index => index.getChildren)
      curCount = nextLevelPartitions.count()
    }

    nextLevelPartitions.cache()
  }

  override def expand(inputRDD: PointRDD): RDD[IndexNode] = levelsExpander(inputRDD)
}
