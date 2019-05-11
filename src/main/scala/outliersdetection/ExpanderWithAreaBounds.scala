package outliersdetection


import util.control.Breaks._
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

import scala.collection.mutable

class ExpanderWithAreaBounds(
                              maxPartitionsToPointsRatio: Double,
                              maxThreshold: Int,
                              minAreaRatio: Double,
                              maxAreaRatio: Double,
                              comparator: IndexNode => Double) extends LevelExpander {
  val parametersList = List(
    (),
    (),
    ()
  )

  private def levelsExpander(rdd: PointRDD): RDD[IndexNode] = {
    val partitionsToPointsRatio = math.min(
      maxPartitionsToPointsRatio,
      maxThreshold.toDouble / rdd.approximateTotalCount
    )

    val area = rdd.boundaryEnvelope.getArea

    val ret = rdd.indexedRDD.rdd
      .map({
        case t: Quadtree => IndexNode(t.getRoot)
        case t: STRtree => IndexNode(t.getRoot)
      })
      .filter(_.getPointsCount > 0)
      .flatMap((initNode: IndexNode) => {
        val numberOfPartitions = initNode.getPointsCount.toDouble * partitionsToPointsRatio

        var initNodes = List[IndexNode](initNode)
        var tmpNodes = List[IndexNode]()
        var break = false

        while (!break && initNodes.nonEmpty &&
          initNodes.size + tmpNodes.size < numberOfPartitions
          && initNodes.exists(_.getBounds.getArea > maxAreaRatio * area)) {

          val (nodesUnderThreshold, nodesAboveThreshold) = initNodes.partition(node => !node.hasChildren || node.getBounds.getArea < maxAreaRatio)

          val newTmp = tmpNodes ::: nodesUnderThreshold
          val newInitNodes = nodesAboveThreshold.flatMap(_.getChildren)

          if (newTmp.size + newInitNodes.size >= numberOfPartitions) {
            break = true
          } else {
            tmpNodes = newTmp
            initNodes = newInitNodes
          }
        }

        initNodes = initNodes ::: tmpNodes

        val expander = mutable.PriorityQueue[IndexNode]()(Ordering.by(comparator))
        initNodes.foreach(expander.enqueue(_))

        var leafNodes: List[IndexNode] = List()

        while(expander.size + leafNodes.size < numberOfPartitions && expander.nonEmpty) {
          val top = expander.dequeue()
          val children = top.getChildren
          if (children.isEmpty || top.getBounds.getArea <= minAreaRatio * area) {
            leafNodes ::= top
          } else {
            children.foreach(expander.enqueue(_))
          }
        }
        expander.toList ::: leafNodes
      }).cache()

    ret
  }


  override def expand(inputRDD: PointRDD): RDD[IndexNode] = {
    levelsExpander(inputRDD)
  }
}
