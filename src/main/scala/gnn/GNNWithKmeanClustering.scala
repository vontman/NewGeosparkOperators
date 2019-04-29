package gnn

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.datasyslab.geospark.spatialRDD.PointRDD

class GNNWithKmeanClustering {

  def solve(geometryFactory: GeometryFactory, inputRDD: PointRDD,
            queryRDD: PointRDD):
  (Point, Double) = {

    val dataRdd = inputRDD.rawSpatialRDD.rdd.cache()
    val queryRdd = queryRDD.rawSpatialRDD.rdd.cache()

    val queryV = queryRdd
      .map(p => Vectors.dense(p.getX, p.getY)).cache()

    val kmeans = KMeans.train(queryV, 10, 5)
    val clusterCenters = kmeans.clusterCenters
    val clusters = kmeans.predict(queryV).map((_, 1)).reduceByKey(_ + _)
      .map { pair => {
        (
          geometryFactory.createPoint(new Coordinate(
            clusterCenters(pair._1)(0),
            clusterCenters(pair._1)(1)
          )),
          pair._2
        )

      }
      }.collect()

    val initResult = dataRdd
      .map(p => (
        p,
        clusters.map(x => p.distance(x._1) * x._2).sum
      ))
      .takeOrdered(10)(Ordering.by(_._2))

    val result = queryRdd
      .flatMap(
        p => initResult.map(f => (f._1, f._1.distance(p))))
      .reduceByKey(_ + _)
      .reduce((a, b) => {
        if (a._2 <= b._2)
          a
        else
          b
      })

    dataRdd.unpersist()
    queryRdd.unpersist()
    result
  }

}
