package example

import com.vividsolutions.jts.geom.{Envelope, Point}
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.strtree.STRtree
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._
import scala.collection.mutable

object OutliersDetection {

  def findOutliersNaive(rdd: PointRDD, k: Int, n: Int): java.util.List[Point] = {
    rdd.buildIndex(IndexType.RTREE, false)
    rdd.indexedRDD.cache()

    val data = rdd.spatialPartitionedRDD.rdd.cache()
    println("Executing native outliers detection")

    data.collect().flatMap(point => {
      KNNQuery.SpatialKnnQuery(rdd, point, k, true).map(p2 => (point.distance(p2), point))
    }).sortBy(_._1).takeRight(n).map(x => x._2).toList
  }

  def findOutliers(rdd: PointRDD, k: Int, n: Int): PointRDD = {

    println("Before #Partitions = " + rdd.grids.length)
    println("Points count before pruning = " + rdd.approximateTotalCount)

    val partitions: List[PartitionProps] = rdd.indexedRDD.filter((index: SpatialIndex) => {
      index.asInstanceOf[STRtree].size != 0
    }).map((index: SpatialIndex) => {
      val partitionProps = new PartitionProps()
      partitionProps.setSize(index.asInstanceOf[STRtree].size())
      partitionProps.setEnvelop(index.asInstanceOf[STRtree].getRoot.getBounds.asInstanceOf[Envelope])

      partitionProps
    }).collect().toList

    val candidates: Iterable[PartitionProps] = computeCandidatePartitions(partitions, k, n)

    val filteredRDD = rdd.indexedRDD.rdd.filter((index: SpatialIndex) => {
      index.asInstanceOf[STRtree].size != 0
    }).filter((partition: SpatialIndex) => {
      val currentPartition = new PartitionProps
      currentPartition.setEnvelop(partition.asInstanceOf[STRtree].getRoot.getBounds.asInstanceOf[Envelope])
      candidates.exists(candidate => candidate.equals(currentPartition))
    }).mapPartitions(indices => indices.flatMap(index => {
      index.query(index.asInstanceOf[STRtree].getRoot.getBounds.asInstanceOf[Envelope]).map(_.asInstanceOf[Point])
    }))


    val newRdd = new PointRDD(filteredRDD)
    newRdd.analyze()

    newRdd.spatialPartitioning(GridType.RTREE)
    newRdd.buildIndex(IndexType.RTREE, true)
    newRdd.indexedRDD.rdd.cache()

    println("After #Partitions = " + candidates.size)
    println("Points count after pruning = " + newRdd.approximateTotalCount)

    newRdd
  }

  private def computeCandidatePartitions(allPartitions: List[PartitionProps], k: Int, n: Int): Iterable[PartitionProps] = {

    val partHeap = mutable.PriorityQueue[PartitionProps]()(Ordering.by(p => -p.lower))
    var minDkDist = 0.0
    var pointsInHeap = 0

    allPartitions.foreach(partition => {
      computeLowerUpper(allPartitions, partition, k, n, minDkDist)

      if (partition.lower > minDkDist) {
        partHeap += partition
        pointsInHeap += partition.size

        while (pointsInHeap - partHeap.head.size >= n) {
          pointsInHeap -= partHeap.dequeue.size
        }
        if (pointsInHeap >= n) {
          minDkDist = partHeap.head.lower
        }
      }
    })

    calculateNeighbours(allPartitions)

    val candidatePartitions = allPartitions.filter((currentPartition: PartitionProps) => {
      currentPartition.upper >= minDkDist
    }).flatMap((currentPartition: PartitionProps) => {
      val partitionWithNeighbours: java.util.List[PartitionProps] = new java.util.ArrayList()
      partitionWithNeighbours.add(currentPartition)
      partitionWithNeighbours.addAll(currentPartition.neighbours)
      return partitionWithNeighbours
    })

    candidatePartitions
  }

  private def computeLowerUpper(allPartitions: List[PartitionProps], partition: PartitionProps, k: Int, n: Int, minDkDist: Double): Unit = {
    partition.lower(Double.MaxValue)
    partition.upper(Double.MaxValue)
    allPartitions.sortBy(p => getMinDist(p.envelop, partition.envelop))

    val lowerHeap = mutable.PriorityQueue[PartitionProps]()(Ordering.by(p => getMinDist(partition.envelop, p.envelop)))
    var lowerPointNum = 0
    val upperHeap = mutable.PriorityQueue[PartitionProps]()(Ordering.by(p => getMaxDist(partition.envelop, p.envelop)))
    var upperPointNum = 0

    allPartitions.foreach(p => {
      if (getMinDist(partition.envelop, p.envelop) < partition.lower) {
        lowerHeap += p
        lowerPointNum += p.size

        while (lowerPointNum - lowerHeap.head.size >= k) {
          lowerPointNum -= lowerHeap.dequeue().size
        }
        if (lowerPointNum >= k) {
          partition.lower(getMinDist(partition.envelop, lowerHeap.head.envelop))
        }
      }

      if (getMaxDist(partition.envelop, p.envelop) < partition.upper) {
        upperHeap += p
        upperPointNum += p.size

        while (upperPointNum - upperHeap.head.size >= k) {
          upperPointNum -= upperHeap.dequeue().size
        }
        if (upperPointNum >= k) {
          partition.upper(getMaxDist(partition.envelop, upperHeap.head.envelop))
        }
        if (partition.upper <= minDkDist) {
          return
        }
      }
    })


  }

  private def getMinDist(env1: Envelope, env2: Envelope): Double = {

    val r = (env1.getMinX, env1.getMinY)
    val rd = (env1.getMaxX, env1.getMaxY)

    val s = (env2.getMinX, env2.getMinY)
    val sd = (env2.getMaxX, env2.getMaxY)

    var dist1 = 0.0
    if (sd._1 < r._1) {
      dist1 += r._1 - sd._1
    } else if (rd._1 < s._1) {
      dist1 += s._1 - rd._1
    } else {
      dist1 += 0
    }

    var dist2 = 0.0
    if (sd._2 < r._2) {
      dist2 += r._2 - sd._2
    } else if (rd._2 < s._2) {
      dist2 += s._2 - rd._2
    } else {
      dist2 += 0
    }

    dist1 * dist1 + dist2 * dist2
  }

  private def getMaxDist(env1: Envelope, env2: Envelope): Double = {
    val r = (env1.getMinX, env1.getMinY)
    val rd = (env1.getMaxX, env1.getMaxY)

    val s = (env2.getMinX, env2.getMinY)
    val sd = (env2.getMaxX, env2.getMaxY)

    val dist1 = Math.max(Math.abs(sd._1 - r._1), Math.abs(rd._1 - s._1))
    val dist2 = Math.max(Math.abs(sd._2 - r._2), Math.abs(rd._2 - s._2))

    dist1 * dist1 + dist2 * dist2
  }

  private def calculateNeighbours(allPartitions: List[PartitionProps]): Unit = {
    allPartitions.foreach((currentPartition: PartitionProps) => {
      currentPartition.addAllNeighbours(
        allPartitions.filter((candidate: PartitionProps) => {
          val env1 = currentPartition.envelop
          val env2 = candidate.envelop

          getMinDist(env1, env2) < currentPartition.upper && env1.intersects(env2)
        })
      )
    })
  }
}