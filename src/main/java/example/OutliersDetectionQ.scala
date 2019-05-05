package example

import java.util

import com.vividsolutions.jts.geom.{Envelope, Point}
import com.vividsolutions.jts.index.quadtree.{Node, NodeBase, Quadtree}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionQ {

  def findOutliers(rdd: PointRDD, k: Int, n: Int, iteration_number: Int, levels: Int): PointRDD = {
    val nextLevelQueryRDD: RDD[NodeBase] = expandLevels(levels, rdd)

    val partitions: List[PartitionProps] = nextLevelQueryRDD.map((node: NodeBase) => {
      val partitionProps = new PartitionProps()
      partitionProps.setSize(node.getPointsCount)
      partitionProps.setEnvelop(node.asInstanceOf[Node].getEnvelope)

      partitionProps
    }).collect().toList

    println("# Partitions before pruning = " + partitions.size)

    Plotter.plotPartitions(rdd.indexedRDD.sparkContext, partitions, "partitionsPlot_" + iteration_number)

    val partitionPropsRdd = rdd.rawSpatialRDD.sparkContext.parallelize(partitions, 8)
      .map(computeLowerUpper(partitions, _, k))
      .sortBy(p => -p.lower)
      .cache()

    //    val candidates: Iterable[PartitionProps] = computeCandidatePartitions(partitions, k, n)
    val candidates: Iterable[PartitionProps] = computeCandidatePartitions(partitionPropsRdd.collect().toList, k, n)

    println("# Partitions after  pruning = " + candidates.size)

    val filteredRDD = nextLevelQueryRDD.filter((node: NodeBase) => {
      val currentPartition = new PartitionProps
      currentPartition.setEnvelop(node.asInstanceOf[Node].getEnvelope)
      candidates.exists(candidate => candidate.equals(currentPartition))
    }).mapPartitions((indices: Iterator[NodeBase]) => {
      indices.flatMap((node: NodeBase) => {
        node.getAllItems
      })
    })

    val newRdd = new PointRDD(filteredRDD)
    newRdd.analyze()

    newRdd.spatialPartitioning(GridType.QUADTREE)
    newRdd.buildIndex(IndexType.QUADTREE, true)
    newRdd.indexedRDD.rdd.cache()

    newRdd
  }

  private def expandLevels(levels: Int, rdd: PointRDD): RDD[NodeBase] = {
    var nextLevelGrids = rdd.indexedRDD.rdd
      .map(_.asInstanceOf[Quadtree].getRoot)
      .map(_.asInstanceOf[NodeBase])
      .filter(_.getPointsCount > 0)
    for (_ <- 0 to levels) {
      nextLevelGrids = nextLevelGrids.flatMap((node: NodeBase) => {
        if (!node.hasChildren) {
          List(node)
        } else {
          if (node.hasItems) {
            val extraBounds: Envelope = node.getItems.map(item => {
              new Envelope(item.asInstanceOf[Point].getCoordinate)
            }).reduce((a, b) => {
              a.expandToInclude(b)
              a
            })
            val extraNode = new Node(extraBounds, 10)
            extraNode.addAllItems(node.getItems)
            extraNode :: node.getSubnode.toList.filter(_ != null)
          } else {
            node.getSubnode.toList.filter(_ != null)
          }
        }
      })
    }
    nextLevelGrids.cache()
  }

  private def computeCandidatePartitions(allPartitions: List[PartitionProps], k: Int, n: Int): Iterable[PartitionProps] = {
    //    //    allPartitions.foreach(partition => computeLowerUpper(allPartitions, partition, k))
    //
    //    val futurePartitions = allPartitions.map(p => Future {
    //      computeLowerUpper(allPartitions, p, k)
    //    })
    //    futurePartitions.foreach(f => {
    //      Await.ready(f, 100 second)
    //    })

    var pointsToTake = n
    //    val minDkDist = allPartitions.sortBy(p => -p.lower).takeWhile(p => {
    val minDkDist = allPartitions.takeWhile(p => {
      if (pointsToTake > 0) {
        pointsToTake -= p.size
        true
      } else {
        false
      }
    }).map(p => p.lower).min

    allPartitions.filter((currentPartition: PartitionProps) => {
      currentPartition.upper >= minDkDist
    }).flatMap((currentPartition: PartitionProps) => {
      val ret = new util.HashSet[PartitionProps]()
      ret.addAll(
        allPartitions
          .filter(p => !p.equals(currentPartition))
          .filter(p => getMinDist(p.envelop, currentPartition.envelop) <= currentPartition.upper)
      )
      ret.add(currentPartition)
      ret
    }).toSet
  }

  private def computeLowerUpper(allPartitions: List[PartitionProps], partition: PartitionProps, k: Int): PartitionProps = {
    var knnVal = k
    partition.lower(
      allPartitions
        .sortBy(p => getMinDist(partition.envelop, p.envelop))
        .takeWhile(p => {
          if (knnVal > 0) {
            knnVal -= p.size
            true
          } else {
            false
          }
        })
        .map(p => getMinDist(partition.envelop, p.envelop)).max
    )


    knnVal = k
    partition.upper(
      allPartitions
        .sortBy(p => getMaxDist(partition.envelop, p.envelop))
        .takeWhile(p => {
          if (knnVal > 0) {
            knnVal -= p.size
            true
          } else {
            false
          }
        })
        .map(p => getMaxDist(partition.envelop, p.envelop)).max
    )
    val ret = new PartitionProps()
    ret.setEnvelop(new Envelope(partition.envelop))
    ret.setSize(partition.size)
    ret.upper(partition.upper)
    ret.lower(partition.lower)
    ret
  }

  private def getMinDist(env1: Envelope, env2: Envelope): Double = {
    if (env1.intersects(env2)) {
      return 0
    }
    val r = (env1.getMinX, env1.getMinY)
    val rd = (env1.getMaxX, env1.getMaxY)

    val s = (env2.getMinX, env2.getMinY)
    val sd = (env2.getMaxX, env2.getMaxY)

    var d1 = 0.0
    if (sd._1 < r._1) {
      d1 = r._1 - sd._1
    } else if (rd._1 < s._1) {
      d1 = s._1 - rd._1
    } else {
      d1 = 0
    }

    var d2 = 0.0
    if (sd._2 < r._2) {
      d2 = r._2 - sd._2
    } else if (rd._2 < s._2) {
      d2 = s._2 - rd._2
    } else {
      d2 = 0
    }

    d1 * d1 + d2 * d2
  }

  private def getMaxDist(env1: Envelope, env2: Envelope): Double = {
    val r = (env1.getMinX, env1.getMinY)
    val rd = (env1.getMaxX, env1.getMaxY)

    val s = (env2.getMinX, env2.getMinY)
    val sd = (env2.getMaxX, env2.getMaxY)

    val d1 = Math.max(Math.abs(sd._1 - r._1), Math.abs(rd._1 - s._1))
    val d2 = Math.max(Math.abs(sd._2 - r._2), Math.abs(rd._2 - s._2))

    d1 * d1 + d2 * d2
  }
}