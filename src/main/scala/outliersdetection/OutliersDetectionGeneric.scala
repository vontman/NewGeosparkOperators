package outliersdetection

import com.vividsolutions.jts.geom.{Envelope, Point}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.mutable

object OutliersDetectionGeneric {
  def apply(gridType: GridType,
            indexType: IndexType,
            levelsExpander: LevelExpander): OutliersDetectionGeneric = {
    new OutliersDetectionGeneric(gridType,
      indexType,
      levelsExpander: LevelExpander)
  }
}

class OutliersDetectionGeneric(gridType: GridType,
                               indexType: IndexType,
                               levelsExpander: LevelExpander)
  extends Serializable {

  def findOutliers(originalBounds: Envelope,
                   inputRDD: PointRDD,
                   n: Int,
                   k: Int,
                   outputPath: String): (Map[String, String], List[Point]) = {

    var logger = Map.empty[String, String]

    val t0 = System.currentTimeMillis()

    inputRDD.analyze()
    inputRDD.spatialPartitioning(gridType)
    inputRDD.buildIndex(indexType, true)

    val partitions = levelsExpander.expand(inputRDD).map(indexNode => {
      val partitionProps = new PartitionProps()
      partitionProps.size(indexNode.getPointsCount)
      partitionProps.envelop(indexNode.getBounds)

      (partitionProps, indexNode)
    }).cache()

    logger += "expanding_partitions_time" -> (System.currentTimeMillis() - t0).toString
    val dataCount = inputRDD.spatialPartitionedRDD.count()

    println("Before # of Points = " + dataCount)
    println("# Partitions before pruning = " + partitions.count())

    val t1 = System.currentTimeMillis()

    val partitionPropsAnalyzed = computeLowerUpper(partitions.map(_._1), k).cache()
    val candidatesToNeighbours = computeCandidatePartitions(partitionPropsAnalyzed, n).cache()

    val remainingPartitionsCount = candidatesToNeighbours.flatMap(_._2).distinct.count()

    println("# Partitions after  pruning = " + remainingPartitionsCount)

    logger += ("neighbour_partitions" -> remainingPartitionsCount.toString)
    logger += ("pruned_partitions" -> (partitions.count() - remainingPartitionsCount).toString)
    logger += ("candidate_partitions" -> candidatesToNeighbours.keys.count.toString)
    logger += "pruning_time" -> (System.currentTimeMillis() - t1).toString

    val candidatePointsToNeighbourPoints =
      candidatesToNeighbours
        .join(partitions)
        .map(e => (e._2._1, e._2._2))
        .cartesian(partitions)
        .groupByKey
        .map(e => {
          val key = e._1
          val value = e._2.toMap
          (key._2, key._1.map(value))
        }).map(e => (e._1.getAllPoints, e._2.flatMap(_.getAllPoints)))
        .map(e => e._1.map((_, e._2)))
        .flatMap(e => e).cache()

    val candidatePointsCount = candidatePointsToNeighbourPoints.keys.count()
    println("After # of candidate Points = " + candidatePointsCount)

    //    println(s"candidates count = $candidatePointsCount, filtered count = $filteredPointsCount, total count = ${inputRDD.approximateTotalCount}")

    //    if (candidatePoints.approximateTotalCount == inputRDD.approximateTotalCount) {
    //      return (logger, candidatePoints)
    //    }
    //
    //    Plotter.visualize(outputPath + "_A", inputRDD.indexedRDD.sparkContext, inputRDD, originalBounds, null)
    //
    //    Plotter.visualize(outputPath + "_B", inputRDD.indexedRDD.sparkContext, candidatePoints, originalBounds)
    //
    //      Plotter.visualize(outputPath + "_C", inputRDD.indexedRDD.sparkContext, candidatePoints, originalBounds, filteredPoints)
    //
    //      Plotter.visualize(outputPath + "_D", inputRDD.indexedRDD.sparkContext, candidatePoints, originalBounds, filteredPoints, partitionsList)

    //    val sc = inputRDD.rawSpatialRDD.sparkContext

    //    val t2 = System.currentTimeMillis()

    //    {
    //      logger += "candidates_percentage" -> ((candidatePointsCount.toDouble / dataCount) * 100.0).toString
    //      val neighboursCnt = neighboursRDD.count().toDouble
    //      logger += "neighbours_percentage" -> ((neighboursCnt / dataCount) * 100.0).toString
    //      logger += "pruning_percentage" -> (((dataCount.toDouble - neighboursCnt) / dataCount) * 100.0).toString
    //    }

    //    reduceOutliersWithKNNJoin(
    //      neighboursRDD,
    //      candidatePointsRDD,
    //      k,
    //      n
    //    )

    //    logger += "reducing_outliers_time_knnjoin" -> (System
    //      .currentTimeMillis() - t2).toString
    //
    //
    //    val t3 = System.currentTimeMillis()
    //    val broadcastEnvsToProps = sc.broadcast(envsToProps)
    //    val neighboursPairRDD = partitions
    //      .filter(indexNode =>
    //        remainingPartitions.contains(broadcastEnvsToProps.value(indexNode.getBounds)))
    //      .map(indexNode => (indexNode, broadcastEnvsToProps.value(indexNode.getBounds).id))
    //
    //    val candidatesWithNeighbours = partitions
    //      .filter(indexNode =>
    //        candidates.contains(broadcastEnvsToProps.value(indexNode.getBounds)))
    //      .flatMap(indexNode =>
    //        indexNode.getAllPoints.map(
    //          (_, broadcastEnvsToProps.value(indexNode.getBounds).neighbours.toSet)))
    //      .collect
    //      .toSet

    val ans = reduceOutliers(
      candidatePointsToNeighbourPoints,
      k,
      n
    )
    //    logger += "reducing_outliers_time" -> (System.currentTimeMillis() - t3).toString
    //    logger += "reducing_outliers_time_custom" -> (System
    //      .currentTimeMillis() - t3).toString
    //    logger += "total_time" -> (System.currentTimeMillis() - t0).toString

    (logger, ans)
  }

  private def computeCandidatePartitions(partitions: RDD[PartitionProps], n: Int) = {
    var pointsToTake = n

    val minDkDist = partitions
      .aggregate((0, new mutable.PriorityQueue[PartitionProps]()(Ordering.by(-_.lower))))((agg, p) => {
        val q = agg._2
        q += p
        var count = agg._1 + p.size

        while (count - q.head.size >= n) {
          count -= q.dequeue().size
        }

        (count, q)
      }, (agg1, agg2) => {
        var count = agg1._1 + agg2._1
        val q = agg1._2
        q ++= agg2._2

        println(s">>>>>>>>>> Q    ==> PointsCount = $count, QueueSize = ${q.size}")
        while (q.nonEmpty && count - q.head.size >= n) {
          count -= q.dequeue().size
        }
        println(s"<<<<<<<<<< Q    ==> PointsCount = $count, QueueSize = ${q.size}")

        (count, q)
      })._2.map(_.lower).min


    partitions
      .filter(p => p.upper >= minDkDist)
      .cartesian(partitions)
      .groupByKey
      .map({ case (p, ps) =>
        (p, ps.filter(cn => getMinDist(cn.envelop, p.envelop) <= p.upper))
      })
  }

  private def computeLowerUpper(partitions: RDD[PartitionProps], k: Int): RDD[PartitionProps] = {

    partitions.cartesian(partitions)
      .groupByKey
      .mapValues(_.toList)
      .map(e => (e._1, e._2.sortBy(q => getMinDist(q.envelop, e._1.envelop))))
      .map(e => {
        var knnVal = k
        e._1.lower(
          e._2.takeWhile(q => {
            if (knnVal > 0) {
              knnVal -= q.size
              true
            } else {
              false
            }
          }).map(q => getMinDist(q.envelop, e._1.envelop))
            .max
        )
        e
      })
      .map(e => (e._1, e._2.sortBy(q => getMaxDist(q.envelop, e._1.envelop))))
      .map(e => {
        var knnVal = k
        e._1.upper(
          e._2.takeWhile(q => {
            if (knnVal > 0) {
              knnVal -= q.size
              true
            } else {
              false
            }
          }).map(q => getMaxDist(q.envelop, e._1.envelop))
            .max
        )
        e
      }).keys
  }

  private def getMinDist(env1: Envelope, env2: Envelope): Double = {

    var ret = 0.0
    var tmp = 0.0

    val (r, rd, s, sd) =
      (env1.getMinX, env1.getMaxX, env2.getMinX, env2.getMaxX)
    tmp = if (sd < r) {
      r - sd
    } else if (rd < s) {
      s - rd
    } else {
      0D
    }
    ret += tmp * tmp

    val (r2, rd2, s2, sd2) =
      (env1.getMinY, env1.getMaxY, env2.getMinY, env2.getMaxY)
    tmp = if (sd2 < r2) {
      r2 - sd2
    } else if (rd2 < s2) {
      s2 - rd2
    } else {
      0D
    }
    ret += tmp * tmp

    ret
  }

  private def getMaxDist(env1: Envelope, env2: Envelope): Double = {

    var ret = 0.0
    var tmp = 0.0

    val (r, rd, s, sd) =
      (env1.getMinX, env1.getMaxX, env2.getMinX, env2.getMaxX)
    tmp = math.max(math.abs(sd - r), math.abs(rd - s))
    ret += tmp * tmp

    val (r2, rd2, s2, sd2) =
      (env1.getMinY, env1.getMaxY, env2.getMinY, env2.getMaxY)
    tmp = math.max(math.abs(sd2 - r2), math.abs(rd2 - s2))
    ret += tmp * tmp

    ret
  }

  private def reduceOutliersWithKNNJoin(neighbours: RDD[Point],
                                        candidates: RDD[Point],
                                        k: Int,
                                        n: Int): List[Point] = {
    val candidatesRDD = new PointRDD(candidates)
    val neighboursRDD = new PointRDD(neighbours)
    candidatesRDD.analyze()
    neighboursRDD.analyze()
    OutliersDetectionNaiveWithKNNJoin.findOutliersNaive2(candidatesRDD,
      k,
      n,
      neighboursRDD)
  }

  private def reduceOutliers(candidateToNeighbours: RDD[(Point, Iterable[Point])],
                             k: Int,
                             n: Int): List[Point] = {
    candidateToNeighbours
      .map(e => (e._1, e._2.toList.sortBy(e._1.distance).take(k).takeRight(1).map(e._1.distance).max))
      .takeOrdered(n)(Ordering.by(-_._2))
      .map(_._1)
      .toList
  }

}
