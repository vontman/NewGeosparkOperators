package example

import com.vividsolutions.jts.geom.{Envelope, Point}
import com.vividsolutions.jts.index.strtree.{AbstractNode, Boundable, STRtree}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._

object OutliersDetectionR {

  def findOutliersNaive(rdd: PointRDD, k: Int, n: Int): java.util.List[Point] = {
    rdd.buildIndex(IndexType.RTREE, false)
    rdd.indexedRDD.cache()

    val data = rdd.spatialPartitionedRDD.rdd.cache()
    println("Executing native outliers detection")

    data.collect().flatMap(point => {
      KNNQuery.SpatialKnnQuery(rdd, point, k, true).map(p2 => (point.distance(p2), point))
    }).sortBy(_._1).takeRight(n).map(x => x._2).toList
  }

  def findOutliers(rdd: PointRDD, k: Int, n: Int, iteration_number: Int): PointRDD = {

    val nextLevelQueryRDD = rdd.indexedRDD.rdd
      .map(_.asInstanceOf[STRtree].getRoot)
      .filter(_.asInstanceOf[Boundable].pointsCount() > 0)
      .flatMap(node => {
        if (node.getLevel == 0) {
          List(node)
        } else {
          node.getChildBoundables.map(node => node.asInstanceOf[AbstractNode])
        }
      }).flatMap(node => {
      if (node.getLevel == 0) {
        List(node)
      } else {
        node.getChildBoundables.map(node => node.asInstanceOf[AbstractNode])
      }
    }).flatMap(node => {
      if (node.getLevel == 0) {
        List(node)
      } else {
        node.getChildBoundables.map(node => node.asInstanceOf[AbstractNode])
      }
    }).cache

    val partitions: List[PartitionProps] = nextLevelQueryRDD.map((node: Boundable) => {
      val partitionProps = new PartitionProps()
      partitionProps.setSize(node.pointsCount())
      partitionProps.setEnvelop(node.getBounds.asInstanceOf[Envelope])

      partitionProps
    }).collect().toList

    println("# Partitions before pruning = " + partitions.size)

    Plotter.plotPartitions(rdd.indexedRDD.sparkContext, partitions, "partitionsPlot_" + iteration_number)

    val candidates: Iterable[PartitionProps] = computeCandidatePartitions(partitions, k, n)

    println("# Partitions after  pruning = " + candidates.size)

    val filteredRDD = nextLevelQueryRDD.filter((node: AbstractNode) => {
      val currentPartition = new PartitionProps
      currentPartition.setEnvelop(node.getBounds.asInstanceOf[Envelope])
      candidates.exists(candidate => candidate.equals(currentPartition))
    }).mapPartitions((indices: Iterator[AbstractNode]) => {
      indices.flatMap(_.getPoints)
    })

    val newRdd = new PointRDD(filteredRDD)
    newRdd.analyze()

    newRdd.spatialPartitioning(GridType.QUADTREE)
    newRdd.buildIndex(IndexType.RTREE, true)
    newRdd.indexedRDD.rdd.cache()

    newRdd
  }

  private def computeCandidatePartitions(allPartitions: List[PartitionProps], k: Int, n: Int): Iterable[PartitionProps] = {
    allPartitions.foreach(partition => computeLowerUpper(allPartitions, partition, k))
    println("Calculated lower and upper bounds")
    var pointsToTake = n
    val minDkDist = allPartitions.sortBy(p => -p.lower).takeWhile(p => {
      if (pointsToTake > 0) {
        pointsToTake -= p.size
        true
      } else {
        false
      }
    }).map(p => p.lower).min

    allPartitions.filter((currentPartition: PartitionProps) => {
      currentPartition.upper >= minDkDist
    })
//      .flatMap((currentPartition: PartitionProps) => {
//      val ret = new util.HashSet[PartitionProps]()
//      ret.addAll(
//        allPartitions
//          .filter(p => !p.equals(currentPartition))
//          .filter(p => getMinDist(p.envelop, currentPartition.envelop) <= currentPartition.upper)
//      )
//      ret.add(currentPartition)
//      ret
//    }).toSet
  }

  private def computeLowerUpper(allPartitions: List[PartitionProps], partition: PartitionProps, k: Int): Unit = {
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
    val l = for {
      x <- List((env1.getMinX, env1.getMinY), (env1.getMinX, env1.getMaxY), (env1.getMaxX, env1.getMinY), (env1.getMaxX, env1.getMaxY))
      y <- List((env2.getMinX, env2.getMinY), (env2.getMinX, env2.getMaxY), (env2.getMaxX, env2.getMinY), (env2.getMaxX, env2.getMaxY))
    }
      yield (x, y)
    l.map(p => {
      val p1 = p._1
      val p2 = p._2

      val d1 = Math.abs(p1._1 - p2._1)
      val d2 = Math.abs(p1._2 - p2._2)
      d1 * d1 + d2 * d2
    }).max
  }
}