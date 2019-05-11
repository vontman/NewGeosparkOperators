package outliersdetection

import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

import scala.collection.mutable

class ExpanderWithAreaBounds(
                              maxThreshold: Int,
                              minArea: Int,
                              maxArea: Int,
                              comparator: IndexNode => Double) extends LevelExpander {
  val parametersList = List(
    (),
    (),
    ()
  )

  private def levelsExpander(rdd: PointRDD): RDD[IndexNode] = {
    val partitionsPerIndex = math.ceil((maxThreshold * 1.0) / rdd.getPartitioner.numPartitions)

    rdd.indexedRDD.rdd
      .map({
        case t: Quadtree => IndexNode(t.getRoot)
        case t: STRtree => IndexNode(t.getRoot)
      })
      .filter(_.getPointsCount > 0)
      .flatMap((initNode: IndexNode) => {
        var initNodes = List(initNode)
        var tmpNodes = List[IndexNode]()
        while (initNodes.nonEmpty &&
          initNodes.size + tmpNodes.size < partitionsPerIndex
          && initNodes.exists(_.getBounds.getArea > maxArea)) {

          val (nodesUnderThreshold, nodesAboveThreshold) = initNodes.partition(node => !node.hasChildren || node.getBounds.getArea < maxArea)
          tmpNodes = tmpNodes ::: nodesUnderThreshold
          initNodes = nodesAboveThreshold.flatMap(_.getChildren)
        }

        initNodes = initNodes ::: tmpNodes

        val expander = mutable.PriorityQueue[IndexNode]()(Ordering.by(comparator))
        initNodes.foreach(expander.enqueue(_))

        var leafNodes: List[IndexNode] = List()

        while (expander.size + leafNodes.size < partitionsPerIndex && expander.nonEmpty) {
          val top = expander.dequeue()
          val children = top.getChildren
          if (children.isEmpty || top.getBounds.getArea <= minArea) {
            leafNodes ::= top
          } else {
            children.foreach(expander.enqueue(_))
          }
        }
        expander.toList ::: leafNodes
      }).cache()
  }


  override def expand(inputRDD: PointRDD): RDD[IndexNode] = {
    levelsExpander(inputRDD)
  }
}
