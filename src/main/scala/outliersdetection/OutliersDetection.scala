package outliersdetection

object OutliersDetection {

  def findOutliers(rdd: PointRDD, k: Int, n: Int): PointRDD = {

    val partitions: List[PartitionProps] = rdd.indexedRDD.rdd.filter(_.asInstanceOf[STRtree].size != 0)
      .map((index: SpatialIndex) => {
        val partitionProps = new PartitionProps()
        partitionProps.setSize(index.asInstanceOf[STRtree].size())
        partitionProps.setEnvelop(index.asInstanceOf[STRtree].getRoot.getBounds.asInstanceOf[Envelope])

        partitionProps
      }).collect().toList


    println("# Partitions before pruning = " + partitions.size)

    Plotter.plotPartitions(rdd.indexedRDD.sparkContext, partitions, "partitionsPlot")

    val candidates: Iterable[PartitionProps] = computeCandidatePartitions(partitions, k, n)

    println("# Partitions after  pruning = " + candidates.size)

    val filteredRDD = rdd.indexedRDD.rdd.filter(_.asInstanceOf[STRtree].size != 0)
      .filter((partition: SpatialIndex) => {
        val currentPartition = new PartitionProps
        currentPartition.setEnvelop(partition.asInstanceOf[STRtree].getRoot.getBounds.asInstanceOf[Envelope])
        candidates.exists(candidate => candidate.equals(currentPartition))
      }).mapPartitions(indices => indices.flatMap(index => {
      index.query(index.asInstanceOf[STRtree].getRoot.getBounds.asInstanceOf[Envelope]).map(_.asInstanceOf[Point])
    }))

    val newRdd = new PointRDD(filteredRDD)
    newRdd.analyze()

    newRdd.spatialPartitioning(GridType.EQUALGRID)
    newRdd.buildIndex(IndexType.RTREE, true)
    newRdd.indexedRDD.rdd.cache()

    newRdd
  }

  private def computeCandidatePartitions(allPartitions: List[PartitionProps], k: Int, n: Int): Iterable[PartitionProps] = {
    allPartitions.foreach(partition => computeLowerUpper(allPartitions, partition, k))

    println("============================================================")
    allPartitions.foreach(p => {
      println(p.size + "\t" + p.lower + "\t" + p.upper + "\t" + p.envelop)
    })

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
        }).map(p => getMinDist(partition.envelop, p.envelop)).max
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
        }).map(p => getMaxDist(partition.envelop, p.envelop)).max
    )
  }

  private def getMinDist(env1: Envelope, env2: Envelope): Double = {

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
