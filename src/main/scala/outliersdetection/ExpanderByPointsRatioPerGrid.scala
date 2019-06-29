package outliersdetection

import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

import scala.collection.mutable

object ExpanderByPointsRatioPerGrid {
  def getPermutations: List[(LevelExpander, String)] = {

    for {
      maxPartitionsRatio <- List(.1)
//      threshold <- List(30000, 15000, 10000, 5000)
      threshold <- List(1000000000)
      (comparator, comparatorName) <- List[(IndexNode => Double, String)](
        (indexNode => indexNode.getBounds.getArea, "area"),
        (indexNode => indexNode.getPointsCount / indexNode.getBounds.getArea, "density"),
        (indexNode => -indexNode.getPointsCount / indexNode.getBounds.getArea, "negDensity"),
        (indexNode => indexNode.getPointsCount, "pointsCount")
      )

    } yield (
      new ExpanderByPointsRatioPerGrid(maxPartitionsRatio, threshold, comparator), s"ExpanderByPointsRatioPerGrid_${maxPartitionsRatio}_${threshold}_${comparatorName}"
    )

  }
}


class ExpanderByPointsRatioPerGrid(maxPartitionsToPointsRatio: Double,
                                   maxThreshold: Int,
                                   queueComparator: IndexNode => Double) extends LevelExpander {

  private def levelsExpander(rdd: PointRDD): RDD[IndexNode] = {
    val partitionsToPointsRatio = math.min(
      maxPartitionsToPointsRatio,
      maxThreshold.toDouble / rdd.approximateTotalCount
    )

    val ret = rdd.indexedRDD.rdd.map({
      case t: Quadtree => IndexNode(t.getRoot)
      case t: STRtree => IndexNode(t.getRoot)
    }).filter(_.getPointsCount > 0)
      .flatMap((initNode: IndexNode) => {

        val numberOfPartitions = initNode.getPointsCount.toDouble * partitionsToPointsRatio
        
        val expander = mutable.PriorityQueue[IndexNode](initNode)(Ordering.by(queueComparator))

        var leafNodes: List[IndexNode] = List()

        while (expander.size + leafNodes.size < numberOfPartitions && expander.nonEmpty) {
          val top = expander.dequeue()
          val children = top.getChildren
          if (children.isEmpty) {
            leafNodes ::= top
          } else {
            children.foreach(expander.enqueue(_))
          }
        }
        expander.toList ::: leafNodes
      }).cache()

    ret
  }

  override def expand(inputRDD: PointRDD): RDD[IndexNode] = levelsExpander(inputRDD)
}
