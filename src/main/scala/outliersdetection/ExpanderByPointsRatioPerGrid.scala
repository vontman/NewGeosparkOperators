package outliersdetection

import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

import scala.collection.mutable

class ExpanderByPointsRatioPerGrid(partitionsPointsInPGridRatio: Double,
                                   queueComparator: IndexNode => Double) extends LevelExpander {

  val parametersList = List(
    0,
    0,
    0
  )

  private def levelsExpander(rdd: PointRDD): RDD[IndexNode] = {
    rdd.indexedRDD.rdd
      .map({
        case t: Quadtree => IndexNode(t.getRoot)
        case t: STRtree => IndexNode(t.getRoot)
      })
      .filter(_.getPointsCount > 0)
      .flatMap((initNode: IndexNode) => {

        val numberOfPartitions = partitionsPointsInPGridRatio * initNode.getPointsCount

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
  }

  override def expand(inputRDD: PointRDD): RDD[IndexNode] = levelsExpander(inputRDD)
}
