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

  def findOutliers(originalBounds: Envelope, inputRDD: PointRDD, n: Int, k: Int, outputPath: String): (Map[String, String], PointRDD) = {

    var logger = Map.empty[String, String]

    val t0 = System.currentTimeMillis()
    //    println(s"Using gridType $gridType, indexType $indexType")

    inputRDD.analyze()
    inputRDD.spatialPartitioning(gridType)
    inputRDD.buildIndex(indexType, true)

    val partitions: RDD[IndexNode] = levelsExpander.expand(inputRDD)
    val dataCount = inputRDD.spatialPartitionedRDD.count()

    try {
      assert(partitions.flatMap(_.getAllPoints).collect.count(_ => true) == inputRDD.approximateTotalCount)
    } catch {
      case e: AssertionError =>
        println(s"Expanded rdd: ${new PointRDD(partitions.flatMap(_.getAllPoints)).approximateTotalCount}, input rdd: ${inputRDD.approximateTotalCount}")
        sys.exit(-1)
    }

    println("Before # of Points = " + dataCount)
    println("# Partitions before pruning = " + partitions.count())
    logger += "used_partitions" -> partitions.count().toString

    val partitionPropsRDD = partitions.map((node: IndexNode) => {
      val partitionProps = new PartitionProps()
      partitionProps.setSize(node.getPointsCount)
      partitionProps.setEnvelop(node.getBounds)

      partitionProps
    }).cache()

    assert(partitionPropsRDD.map(_.size).sum == inputRDD.approximateTotalCount)

    val partitionsList = partitionPropsRDD.collect().toList
    val partitionPropsAnalyzed = partitionPropsRDD.map(computeLowerUpper(partitionsList, _, k)).cache()

    val candidates: Set[PartitionProps] = computeCandidatePartitions(partitionPropsAnalyzed, n).collect.toSet

    println("# Partitions after  pruning = " + candidates.size)
    logger += "partitions_after_pruning" -> candidates.size.toString

    val candidatePointsRDD: RDD[Point] = partitions.filter((node: IndexNode) => {
      val currentPartition = new PartitionProps
      currentPartition.setEnvelop(node.getBounds)
      candidates.contains(currentPartition)
    }).mapPartitions(_.flatMap(_.getAllPoints))

    val filteredPointsRDD: RDD[Point] = partitions.filter((node: IndexNode) => {
      val currentPartition = new PartitionProps
      currentPartition.setEnvelop(node.getBounds)
      !candidates.contains(currentPartition)
    }).mapPartitions(_.flatMap(_.getAllPoints))

    logger += "time" -> (System.currentTimeMillis() - t0).toString

    val candidatePoints = new PointRDD(candidatePointsRDD)
    val filteredPoints = new PointRDD(filteredPointsRDD)
    val candidatePointsCount = candidatePoints.rawSpatialRDD.count()

    println("After # of Points = " + candidatePointsCount)

    Array(candidatePoints, filteredPoints).foreach(rdd => {
      rdd.analyze()
      //      try {
      //        rdd.spatialPartitioning(gridType)
      //        rdd.buildIndex(indexType, true)
      //      } catch {
      //        case _: Exception =>
      //      }
    })

    println(s"candidates count = ${candidatePoints.approximateTotalCount}, filtered count = ${filteredPoints.approximateTotalCount}, total count = ${inputRDD.approximateTotalCount}")
    assert(candidatePoints.approximateTotalCount + filteredPoints.approximateTotalCount == inputRDD.approximateTotalCount)

    if (candidatePoints.approximateTotalCount == inputRDD.approximateTotalCount) {
      return (logger, candidatePoints)
    }

    //    Plotter.visualize(outputPath + "_A", inputRDD.indexedRDD.sparkContext, inputRDD, originalBounds, null)

    //    Plotter.visualize(outputPath + "_B", inputRDD.indexedRDD.sparkContext, candidatePoints, originalBounds)

//    Plotter.visualize(outputPath + "_C", inputRDD.indexedRDD.sparkContext, candidatePoints, originalBounds, filteredPoints)
//
//    Plotter.visualize(outputPath + "_D", inputRDD.indexedRDD.sparkContext, candidatePoints, originalBounds, filteredPoints, partitionsList)

    (logger, candidatePoints)
  }

  private def computeCandidatePartitions(allPartitions: RDD[PartitionProps], n: Int): RDD[PartitionProps] = {
//    var pointsToTake = n

//    val minDkDist = allPartitions
//      .sortBy(-_.lower)
//      .collect()
//      .takeWhile(p => {
//        if (pointsToTake > 0) {
//          pointsToTake -= p.size
//          true
//        } else {
//          false
//        }
//      }).map(_.lower).min
    val minDkDist = allPartitions
    .flatMap(p => {
      List.fill(p.size)(-p.lower)
    }).takeOrdered(n).max * -1


    allPartitions.filter((currentPartition: PartitionProps) => {
      currentPartition.upper >= minDkDist
    })
//          .flatMap((currentPartition: PartitionProps) => {
//          allPartitions
//            .filter(p => !p.equals(currentPartition))
//            .filter(p => getMinDist(p.envelop, currentPartition.envelop) <= currentPartition.upper)
//        })
  }

  private def computeLowerUpper(allPartitions: List[PartitionProps], partition: PartitionProps, k: Int): PartitionProps = {
    val analyzedPartition = new PartitionProps
    analyzedPartition.setEnvelop(partition.envelop)
    analyzedPartition.setSize(partition.size)

    var knnVal = k
    analyzedPartition.lower(
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
    analyzedPartition.upper(
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

    analyzedPartition
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
    ret += tmp*tmp

    val (r2, rd2, s2, sd2) = (env1.getMinY, env1.getMaxY, env2.getMinY, env2.getMaxY)
    tmp = if (sd2 < r2) {
      r2 - sd2
    } else if (rd2 < s2) {
      s2 - rd2
    } else {
      0D
    }
    ret += tmp*tmp

    ret
  }

  private def getMaxDist(env1: Envelope, env2: Envelope): Double = {

    var ret = 0.0
    var tmp = 0.0

    val (r, rd, s, sd) = (env1.getMinX, env1.getMaxX, env2.getMinX, env2.getMaxX)
    tmp = math.max(math.abs(sd - r), math.abs(rd - s))
    ret += tmp*tmp

    val (r2, rd2, s2, sd2) = (env1.getMinY, env1.getMaxY, env2.getMinY, env2.getMaxY)
    tmp = math.max(math.abs(sd2 - r2), math.abs(rd2 - s2))
    ret += tmp*tmp

    ret
  }
}
