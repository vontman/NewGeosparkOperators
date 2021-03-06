package outliersdetection

import com.vividsolutions.jts.geom.{Envelope, Point}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

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
    //    println(s"Using gridType $gridType, indexType $indexType")

    inputRDD.analyze()
    inputRDD.spatialPartitioning(gridType)
    inputRDD.buildIndex(indexType, true)

    val partitions: RDD[IndexNode] = levelsExpander.expand(inputRDD).cache()
    logger += "expanding_partitions_time" -> (System.currentTimeMillis() - t0).toString
    val dataCount = inputRDD.spatialPartitionedRDD.count()

    println("Before # of Points = " + dataCount)
    println("# Partitions before pruning = " + partitions.count())

    val t1 = System.currentTimeMillis()

    val partitionPropsRDD = partitions.zipWithIndex
      .map({
        case (indexNode, id) =>
          val partitionProps = new PartitionProps()
          partitionProps.id(id.toInt)
          partitionProps.size(indexNode.getPointsCount)
          partitionProps.envelop(indexNode.getBounds)

          partitionProps
      })
      .cache()

    var partitionsList = partitionPropsRDD.collect.toList

    val partitionPropsAnalyzed =
      partitionPropsRDD.map(computeLowerUpper(partitionsList, _, k)).cache()
    val tmp = computeCandidatePartitions(partitionPropsAnalyzed.collect, n)
    partitionsList = tmp._1.toList
    val candidates: Set[PartitionProps] = tmp._2.toSet

    val envsToProps = partitionsList.map(p => (p.envelop, p)).toMap
    val idToProps = partitionsList.map(p => (p.id, p)).toMap

    val remainingPartitions = candidates.flatMap(_.neighbours).map(idToProps)
    val remainingPartitionsCount = candidates.flatMap(_.neighbours).size

    println("# Partitions after  pruning = " + remainingPartitionsCount)

    logger += ("neighbour_partitions" -> remainingPartitionsCount.toString)
    logger += ("pruned_partitions" -> (partitions.count() - remainingPartitionsCount).toString)
    logger += ("candidate_partitions" -> candidates.size.toString)

    val candidatePointsRDD: RDD[Point] = partitions
      .filter((node: IndexNode) => {
        val currentPartition = new PartitionProps
        currentPartition.envelop(node.getBounds)
        candidates.contains(currentPartition)
      })
      .mapPartitions(_.flatMap(_.getAllPoints))

    //    val filteredPointsRDD: RDD[Point] = partitions.filter((node: IndexNode) => {
    //      val currentPartition = new PartitionProps
    //      currentPartition.envelop(node.getBounds)
    //      filteredPartitions.contains(currentPartition)
    //    }).mapPartitions(_.flatMap(_.getAllPoints))

    logger += "pruning_time" -> (System.currentTimeMillis() - t1).toString

    //    val candidatePoints = new PointRDD(candidatePointsRDD)
    //    val filteredPoints = new PointRDD(filteredPointsRDD)

    //    Array(candidatePoints, filteredPoints).foreach(_.analyze)

    val candidatePointsCount = candidatePointsRDD.count()
    //    val filteredPointsCount = filteredPoints.approximateTotalCount
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

    val sc = inputRDD.rawSpatialRDD.sparkContext

    val t2 = System.currentTimeMillis()
    val neighboursRDD = partitions
      .filter(indexNode =>
        remainingPartitions.contains(envsToProps(indexNode.getBounds)))
      .flatMap(_.getAllPoints)
      .cache()

    {
      logger += "candidates_percentage" -> ((candidatePointsCount.toDouble / dataCount) * 100.0).toString
      val neighboursCnt = neighboursRDD.count().toDouble
      logger += "neighbours_percentage" -> ((neighboursCnt / dataCount) * 100.0).toString
      logger += "pruning_percentage" -> (((dataCount.toDouble - neighboursCnt) / dataCount) * 100.0).toString
    }

    reduceOutliersWithKNNJoin(
      neighboursRDD,
      candidatePointsRDD,
      k,
      n
    )

    logger += "reducing_outliers_time_knnjoin" -> (System
      .currentTimeMillis() - t2).toString


    val t3 = System.currentTimeMillis()
    val broadcastEnvsToProps = sc.broadcast(envsToProps)
    val neighboursPairRDD = partitions
      .filter(indexNode =>
        remainingPartitions.contains(broadcastEnvsToProps.value(indexNode.getBounds)))
      .map(indexNode => (indexNode, broadcastEnvsToProps.value(indexNode.getBounds).id))

    val candidatesWithNeighbours = partitions
      .filter(indexNode =>
        candidates.contains(broadcastEnvsToProps.value(indexNode.getBounds)))
      .flatMap(indexNode =>
        indexNode.getAllPoints.map(
          (_, broadcastEnvsToProps.value(indexNode.getBounds).neighbours.toSet)))
      .collect
      .toSet

    val ans = reduceOutliers(
      neighboursPairRDD,
      candidatesWithNeighbours,
      k,
      n
    )
    logger += "reducing_outliers_time" -> (System.currentTimeMillis() - t3).toString
    logger += "reducing_outliers_time_custom" -> (System
      .currentTimeMillis() - t3).toString
    logger += "total_time" -> (System.currentTimeMillis() - t0).toString

    (logger, ans)
  }

  private def computeCandidatePartitions(
                                          allPartitions: Iterable[PartitionProps],
                                          n: Int): (Iterable[PartitionProps], Iterable[PartitionProps]) = {
    var pointsToTake = n

    val minDkDist = allPartitions.toList
      .sortBy(-_.lower)
      .takeWhile(p => {
        if (pointsToTake > 0) {
          pointsToTake -= p.size
          true
        } else {
          false
        }
      })
      .map(_.lower)
      .min

    allPartitions.foreach(p => p.neighbours(Set(p.id)))

    val candidatePartitions: Iterable[PartitionProps] =
      allPartitions.filter(_.upper >= minDkDist)
    candidatePartitions.foreach(c => {
      c.neighbours(
        allPartitions
          .filter(cn => getMinDist(cn.envelop, c.envelop) <= c.upper)
          .map(_.id)
          .toSet)
    })

    (allPartitions, candidatePartitions)
  }

  private def computeLowerUpper(allPartitions: List[PartitionProps],
                                partition: PartitionProps,
                                k: Int): PartitionProps = {

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
        .map(p => getMinDist(partition.envelop, p.envelop))
        .max
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
        .map(p => getMaxDist(partition.envelop, p.envelop))
        .max
    )

    partition
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

  private def reduceOutliers(neighbours: RDD[(IndexNode, Int)],
                             candidates: Iterable[(Point, Set[Int])],
                             k: Int,
                             n: Int): List[Point] = {
    neighbours
      .mapPartitions(
        _.flatMap(x => {
          val indexNode = x._1
          val id = x._2
          candidates
            .view
            .filter(_._2.contains(id))
            .map(
              c =>
                (
                  c._1,
                  indexNode.getAllPoints
                    .sortBy(c._1.distance)
                    .take(k)
                    .map(p => p.distance(c._1))
                ))
            .toList
        })
      )
      .groupByKey()
      .mapValues(knnsIter => {
        val knns = knnsIter.toArray
        var res = Double.MaxValue
        val ids = Array.fill(knns.length)(0)

        for (_ <- 0 until k) {
          var chosenList = -1
          var best = 0.0

          for (j <- knns.indices) {
            val l = knns(j)
            val index = ids(j)
            if (index < l.length) {
              val v = l(index)
              if (chosenList == -1 || best > v) {
                best = v
                chosenList = j
              }
            }
          }

          if (chosenList != -1) {
            res = best
            ids(chosenList) += 1
          }
        }

        res
      })
      .takeOrdered(n)(
        Ordering.by((g: (Point, Double)) => g._2).reverse
      )
      .map(_._1)
      .toList
  }

}
