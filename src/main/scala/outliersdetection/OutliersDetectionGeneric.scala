package outliersdetection

import java.util

import com.vividsolutions.jts.geom.{Envelope, Point}
import com.vividsolutions.jts.index.quadtree.Quadtree
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

import scala.collection.JavaConversions._
import scala.collection.mutable

object OutliersDetectionGeneric {
  def apply(gridType: GridType, indexType: IndexType, maxNumberOfPartitions: Int) = {
    new OutliersDetectionGeneric(gridType, indexType, maxNumberOfPartitions)
  }

}

class OutliersDetectionGeneric(gridType: GridType, indexType: IndexType, maxNumberOfPartitions: Int) extends Serializable {

  def findOutliers(inputRDD: PointRDD, k: Int, n: Int, iteration_number: Int): PointRDD = {

    println(s"Using gridType $gridType, indexType $indexType")

    inputRDD.analyze()
    inputRDD.spatialPartitioning(gridType)
    inputRDD.buildIndex(indexType, true)

    val originalBounds = inputRDD.boundaryEnvelope
    val partitions: RDD[IndexNode] = expandLevels(inputRDD, maxNumberOfPartitions)
    //    assert(new PointRDD(partitions.flatMap(_.getAllPoints)).countWithoutDuplicates() == inputRDD.countWithoutDuplicates())

    println("Before # of Points = " + inputRDD.countWithoutDuplicates())
    println("# Partitions before pruning = " + partitions.count())

    val partitionPropsRDD = partitions.map((node: IndexNode) => {
      val partitionProps = new PartitionProps()
      partitionProps.setSize(node.getPointsCount)
      partitionProps.setEnvelop(node.getBounds)

      partitionProps
    }).cache()

    val partitionsList = partitionPropsRDD.collect().toList

    val partitionPropsRdd = partitionPropsRDD.map(computeLowerUpper(partitionsList, _, k)).sortBy(-_.lower).cache()

    //    val candidates: Iterable[PartitionProps] = computeCandidatePartitions(partitions, k, n)
    val candidates = computeCandidatePartitions(partitionPropsRdd.collect().toList, k, n)

    println("# Partitions after  pruning = " + candidates.size)

    val candidatePointsRDD: RDD[Point] = partitions.filter((node: IndexNode) => {
      val currentPartition = new PartitionProps
      currentPartition.setEnvelop(node.getBounds)
      candidates.contains(currentPartition)
    }).mapPartitions((indices: Iterator[IndexNode]) => {
      indices.flatMap((node: IndexNode) => {
        node.getAllPoints
      })
    })

    val filteredPointsRDD: RDD[Point] = partitions.filter((node: IndexNode) => {
      val currentPartition = new PartitionProps
      currentPartition.setEnvelop(node.getBounds)
      !candidates.contains(currentPartition)
    }).mapPartitions((indices: Iterator[IndexNode]) => {
      indices.flatMap((node: IndexNode) => {
        node.getAllPoints
      })
    })

    val candidatePoints = new PointRDD(candidatePointsRDD)
    val filteredPoints = new PointRDD(filteredPointsRDD)

    println("After # of Points = " + candidatePoints.countWithoutDuplicates())

    Array(candidatePoints, filteredPoints).foreach(rdd => {
      rdd.analyze()
      try {
        rdd.spatialPartitioning(gridType)
        rdd.buildIndex(indexType, true)
      } catch {
        case _: Exception =>
      }
    })

    assert(candidatePoints.rawSpatialRDD.count() + filteredPoints.rawSpatialRDD.count() == inputRDD.rawSpatialRDD.count())

    if (candidatePoints.countWithoutDuplicates() == inputRDD.countWithoutDuplicates()) {
      return candidatePoints
    }

    Plotter.visualize(inputRDD.indexedRDD.sparkContext, inputRDD, "Iteration_" + iteration_number + "_A", originalBounds)

    Plotter.visualize(inputRDD.indexedRDD.sparkContext, candidatePoints, "Iteration_" + iteration_number + "_B", originalBounds)

    Plotter.visualize(inputRDD.indexedRDD.sparkContext, candidatePoints, "Iteration_" + iteration_number + "_C", originalBounds, filteredPoints)

    Plotter.visualize(inputRDD.indexedRDD.sparkContext, candidatePoints, "Iteration_" + iteration_number + "_D", originalBounds, filteredPoints, partitionsList)

    candidatePoints
  }

  private def expandLevels(rdd: PointRDD, maxNumberOfPartitions: Int): RDD[IndexNode] = {
    val partitionsPerIndex = math.ceil((maxNumberOfPartitions * 1.0) / rdd.getPartitioner.numPartitions)

    val AREA_MAX = rdd.boundaryEnvelope.getArea / 300
    val AREA_MIN = rdd.boundaryEnvelope.getArea / 3000

    rdd.indexedRDD.rdd
      .map({
        case t: Quadtree => IndexNode(t.getRoot)
        case t: STRtree => IndexNode(t.getRoot)
      })
      .filter(_.getPointsCount > 0)
      .flatMap((initNode: IndexNode) => {
        var initNodes = List(initNode)
        var tmpNodes = List[IndexNode]()
        while (initNodes.nonEmpty && initNodes.size + tmpNodes.size < partitionsPerIndex && initNodes.exists(_.getBounds.getArea > AREA_MAX)) {
          val (nodesUnderThreshold, nodesAboveThreshold) = initNodes.partition(node => !node.hasChildren || node.getBounds.getArea < AREA_MAX)
          tmpNodes = tmpNodes ::: nodesUnderThreshold
          initNodes = nodesAboveThreshold.flatMap(_.getChildren)
        }

        initNodes = initNodes ::: tmpNodes

        val expander = mutable.PriorityQueue[IndexNode]()(Ordering.by(node => node.getBounds.getArea))
        initNodes.foreach(expander.enqueue(_))

        var leafNodes: List[IndexNode] = List()

        while (expander.size + leafNodes.size < partitionsPerIndex && expander.nonEmpty) {
          val top = expander.dequeue()
          val children = top.getChildren
          if (children.isEmpty || top.getBounds.getArea <= AREA_MIN) {
            leafNodes ::= top
          } else {
            children.foreach(expander.enqueue(_))
          }
        }
        expander.toList ::: leafNodes
      }).cache()
  }

  private def computeCandidatePartitions(allPartitions: List[PartitionProps], k: Int, n: Int) = {
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
    if (env1.contains(env2) || env2.contains(env1)) {
      return 0
    }
    val r = (env1.getMinX, env1.getMinY)
    val rd = (env1.getMaxX, env1.getMaxY)

    val s = (env2.getMinX, env2.getMinY)
    val sd = (env2.getMaxX, env2.getMaxY)

    val d1 = Math.max(Math.abs(sd._1 - r._1), Math.abs(rd._1 - s._1))
    val d2 = Math.max(Math.abs(sd._2 - r._2), Math.abs(rd._2 - s._2))

    d1 * d1 + d2 * d2
  }
}