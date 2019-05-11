package outliersdetection


import util.control.Breaks._
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

import scala.collection.mutable

object ExpanderWithAreaBounds {
  def getPermutations: List[(LevelExpander, String)] = {

    for {
      maxPartitionsRatio <- List(.1)
      threshold <- List(30000, 15000, 10000, 5000)
      minAreaRatio <- List(1.0 / 300)
      maxAreaRatio <- List(1.0 / 5000)
      (comparator, comparatorName) <- List[(IndexNode => Double, String)](
        (indexNode => indexNode.getBounds.getArea, "area"),
        (indexNode => indexNode.getPointsCount / indexNode.getBounds.getArea, "density"),
        (indexNode => -indexNode.getPointsCount / indexNode.getBounds.getArea, "negDensity"),
        (indexNode => indexNode.getPointsCount, "pointsCount")
      )

    } yield (
      new ExpanderWithAreaBounds(maxPartitionsRatio, threshold, minAreaRatio, maxAreaRatio, comparator), s"ExpanderWithAreaBounds_${maxPartitionsRatio}_${threshold}_${minAreaRatio}_${maxAreaRatio}_${comparatorName}"
    )

  }
}

class ExpanderWithAreaBounds(
                              maxPartitionsToPointsRatio: Double,
                              maxThreshold: Int,
                              minAreaRatio: Double,
                              maxAreaRatio: Double,
                              comparator: IndexNode => Double) extends LevelExpander {
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

        while(initNodes.size + tmpNodes.size < numberOfPartitions && initNodes.nonEmpty) {
          val head = initNodes.head
          initNodes = initNodes.tail
          val children = head.getChildren
          if (head.getBounds.getArea <= maxAreaRatio * area || children.isEmpty) {
            tmpNodes ::= head
          } else {
            initNodes = initNodes ::: children
          }
        }

        initNodes = initNodes ::: tmpNodes
//        println("First Stage " + initNodes.size)

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

//        println("SecondStage " + (expander.size + leafNodes.size))
        expander.toList ::: leafNodes

      }).cache()

    ret
  }


  override def expand(inputRDD: PointRDD): RDD[IndexNode] = {
    levelsExpander(inputRDD)
  }
}
