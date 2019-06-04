package outliersdetection

import com.vividsolutions.jts.geom.{Envelope, Point}
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

object OutliersDetectionGeneric {
  def apply(gridType: GridType, indexType: IndexType, levelsExpander: LevelExpander): OutliersDetectionGeneric = {
    new OutliersDetectionGeneric(gridType, indexType, levelsExpander: LevelExpander)
  }

}

class OutliersDetectionGeneric(gridType: GridType, indexType: IndexType, levelsExpander: LevelExpander) extends Serializable {

  def findOutliers(originalBounds: Envelope, inputRDD: PointRDD, n: Int, k: Int, outputPath: String): (Map[String, String], Iterable[Point]) = {

    var logger = Map.empty[String, String]

    val t0 = System.currentTimeMillis()
    //    println(s"Using gridType $gridType, indexType $indexType")

    inputRDD.analyze()
    inputRDD.spatialPartitioning(gridType)
    inputRDD.buildIndex(indexType, true)

    val partitions: RDD[IndexNode] = levelsExpander.expand(inputRDD)
    val dataCount = inputRDD.spatialPartitionedRDD.count()

    println("Before # of Points = " + dataCount)
    println("# Partitions before pruning = " + partitions.count())
    logger += "used_partitions" -> partitions.count().toString

    val partitionPropsRDD = partitions.zipWithIndex.map({ case (indexNode, id) =>
      val partitionProps = new PartitionProps()
      partitionProps.id(id.toInt)
      partitionProps.size(indexNode.getPointsCount)
      partitionProps.envelop(indexNode.getBounds)

      partitionProps
    }).cache()

    var partitionsList = partitionPropsRDD.collect.toList

    val partitionPropsAnalyzed = partitionPropsRDD.map(computeLowerUpper(partitionsList, _, k))
    val tmp = computeCandidatePartitions(partitionPropsAnalyzed.collect, n)
    partitionsList = tmp._1.toList
    val candidates: Set[PartitionProps] = tmp._2.toSet

    val envsToProps = partitionsList.map(p => (p.envelop, p)).toMap
    val idToProps = partitionsList.map(p => (p.id, p)).toMap

    val remainingPartitions = candidates.flatMap(_.neighbours).map(idToProps)
    val filteredPartitions = partitionsList.filterNot(remainingPartitions.contains)

    println("# Partitions after  pruning = " + candidates.flatMap(_.neighbours).size)
    logger += "partitions_after_pruning" -> candidates.size.toString

    val candidatePointsRDD: RDD[Point] = partitions.filter((node: IndexNode) => {
      val currentPartition = new PartitionProps
      currentPartition.envelop(node.getBounds)
      candidates.contains(currentPartition)
    }).mapPartitions(_.flatMap(_.getAllPoints))

    val filteredPointsRDD: RDD[Point] = partitions.filter((node: IndexNode) => {
      val currentPartition = new PartitionProps
      currentPartition.envelop(node.getBounds)
      filteredPartitions.contains(currentPartition)
    }).mapPartitions(_.flatMap(_.getAllPoints))

    logger += "time" -> (System.currentTimeMillis() - t0).toString

    val candidatePoints = new PointRDD(candidatePointsRDD)
    val filteredPoints = new PointRDD(filteredPointsRDD)

    Array(candidatePoints, filteredPoints).foreach(_.analyze)

    val candidatePointsCount = candidatePoints.approximateTotalCount
    val filteredPointsCount = filteredPoints.approximateTotalCount
    println("After # of Points = " + candidatePointsCount)


    println(s"candidates count = $candidatePointsCount, filtered count = $filteredPointsCount, total count = ${inputRDD.approximateTotalCount}")
    assert(candidatePointsCount + filteredPointsCount == inputRDD.approximateTotalCount)

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

    val ans = reduceOutliers(
      partitions.filter(indexNode => remainingPartitions.contains(envsToProps(indexNode.getBounds))).map(indexNode => (indexNode, envsToProps(indexNode.getBounds).id)),
      partitions.filter(indexNode => candidates.contains(envsToProps(indexNode.getBounds))).flatMap(indexNode => indexNode.getAllPoints.map((_, envsToProps(indexNode.getBounds).neighbours.toSet))).collect.toSet,
      k,
      n
    )


    (logger, ans)
  }

  private def computeCandidatePartitions(allPartitions: Iterable[PartitionProps], n: Int): (Iterable[PartitionProps], Iterable[PartitionProps]) = {
    var pointsToTake = n

    val minDkDist = allPartitions
      .toList.sortBy(-_.lower)
      .takeWhile(p => {
        if (pointsToTake > 0) {
          pointsToTake -= p.size
          true
        } else {
          false
        }
      }).map(_.lower).min

    allPartitions.foreach(p => p.neighbours(Set(p.id)))

    val candidatePartitions: Iterable[PartitionProps] = allPartitions.filter(_.upper >= minDkDist)
    candidatePartitions.foreach(c => {
      c.neighbours(allPartitions.filter(cn => getMinDist(cn.envelop, c.envelop) <= c.upper).map(_.id).toSet)
    })

    (allPartitions, candidatePartitions)
  }

  private def computeLowerUpper(allPartitions: List[PartitionProps], partition: PartitionProps, k: Int): PartitionProps = {

    var knnVal = k
    partition.lower(
      allPartitions.sortBy(p => getMinDist(partition.envelop, p.envelop))
        .takeWhile(p => {
          if (knnVal > 0) {
            knnVal -= p.size
            true
          } else {
            false
          }
        }).map(p => getMinDist(partition.envelop, p.envelop)).max
    )


    knnVal = k
    partition.upper(
      allPartitions.sortBy(p => getMaxDist(partition.envelop, p.envelop))
        .takeWhile(p => {
          if (knnVal > 0) {
            knnVal -= p.size
            true
          } else {
            false
          }
        }).map(p => getMaxDist(partition.envelop, p.envelop)).max
    )

    partition
  }

  private def getMinDist(env1: Envelope, env2: Envelope): Double = {

    var ret = 0.0
    var tmp = 0.0

    val (r, rd, s, sd) = (env1.getMinX, env1.getMaxX, env2.getMinX, env2.getMaxX)
    tmp = if (sd < r) {
      r - sd
    } else if (rd < s) {
      s - rd
    } else {
      0D
    }
    ret += tmp * tmp

    val (r2, rd2, s2, sd2) = (env1.getMinY, env1.getMaxY, env2.getMinY, env2.getMaxY)
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

    val (r, rd, s, sd) = (env1.getMinX, env1.getMaxX, env2.getMinX, env2.getMaxX)
    tmp = math.max(math.abs(sd - r), math.abs(rd - s))
    ret += tmp * tmp

    val (r2, rd2, s2, sd2) = (env1.getMinY, env1.getMaxY, env2.getMinY, env2.getMaxY)
    tmp = math.max(math.abs(sd2 - r2), math.abs(rd2 - s2))
    ret += tmp * tmp

    ret
  }

  private def reduceOutliers(neighbours: RDD[(IndexNode, Int)], candidates: Iterable[(Point, Set[Int])], k: Int, n: Int): List[Point] = {
    neighbours.flatMap[(Point, List[(Double, Point)])]({
      case (indexNode, id) =>
        candidates.filter(_._2.contains(id))
          .map(c => (c._1, indexNode.getAllPoints.sortBy(c._1.distance).take(k).map(p => (p.distance(c._1), p))))
    }).reduceByKey((l1, l2) => {
      (l1 ::: l2).sortBy(_._1).take(k)
    }).map({
      case (c, ns) => (c, ns.last._1)
    })
      .sortBy(_._2, ascending = false)
      .take(n).map(_._1).toList
  }

}
