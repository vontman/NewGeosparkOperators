package gnn

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._

object GNNApprox extends GNNSolver {

  override def solve(geometryFactory: GeometryFactory, inputSpatialRdd: PointRDD,
                     querySpatialRdd: PointRDD, resultStr: StringBuilder, visualize: Boolean,
                     outputPath: String):
  (Map[String, String], (Point, Double)) = {
    querySpatialRdd.spatialPartitioning(GridType.RTREE)
    querySpatialRdd.buildIndex(IndexType.RTREE, true)

    val inputRDD = inputSpatialRdd.rawSpatialRDD.rdd.cache()
    val queryRDD = querySpatialRdd.indexedRDD.rdd.cache()

    val min_points = queryRDD.mapPartitions(spatialIndices => {
      var xSum = 0.0
      var ySum = 0.0
      var count = 0
      spatialIndices.foreach(index => {
        val bounds: Envelope = index.asInstanceOf[STRtree].getRoot
          .getBounds
          .asInstanceOf[Envelope]
        //        println("Depth:" + index.asInstanceOf[STRtree].depth())
        //        println("Envelope:" + bounds)
        //        println("Items:" + index.query(bounds))
        val points = index.query(bounds).toArray()
        points.foreach(p => {
          val q = p.asInstanceOf[Point]
          xSum += q.getX
          ySum += q.getY
          count += 1
        })
      })
      List(
        (geometryFactory.createPoint(new Coordinate(xSum / count, ySum
          / count)), count)
      ).iterator
    }).collect()

    //    println("Using min points: " + min_points.size)
    //    min_points.foreach(println)

    val initResult = inputRDD
      .map(p => (
        p,
        min_points.map(x => p.distance(x._1) * x._2).sum
      ))
      .takeOrdered(10)(Ordering.by(_._2))
    //

    val result = queryRDD
      .flatMap(
        index =>
          index.query(index.asInstanceOf[STRtree].getRoot.getBounds
            .asInstanceOf[Envelope]).toList
            .flatMap(
              p => initResult.map
              (f => (f._1, f._1.distance(p.asInstanceOf[Point])))
            )
      )
      .reduceByKey(_ + _)
      .reduce((a, b) => {
        if (a._2 <= b._2)
          a
        else
          b
      })

    (Map(), result)
  }

}
