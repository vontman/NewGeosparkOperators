//package knnjoin
//
//import com.vividsolutions.jts.geom
//import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
//import com.vividsolutions.jts.io.WKTReader
//import cs.purdue.edu.spatialindex.rtree.Point
//import cs.purdue.edu.spatialrdd.SpatialRDD
//import cs.purdue.edu.spatialrdd.impl.{Util, knnJoinRDD}
//import org.apache.spark.api.java.JavaPairRDD
//
//import scala.util.Try
//
//object KNNJoinWithLocationSpark extends KNNJoinSolver {
//  override def solve(
//      geometryFactory: GeometryFactory,
//      dataRDD: org.datasyslab.geospark.spatialRDD.SpatialRDD[
//        com.vividsolutions.jts.geom.Point],
//      queryRDD: org.datasyslab.geospark.spatialRDD.SpatialRDD[
//        com.vividsolutions.jts.geom.Point],
//      k: Int,
//      resultStr: StringBuilder,
//      visualize: Boolean,
//      outputPath: String): JavaPairRDD[com.vividsolutions.jts.geom.Point,
//                                       com.vividsolutions.jts.geom.Point] = {
//
////    val leftRDD = SpatialRDD(dataRDD.rawSpatialRDD.rdd.map[(Point, String)](p =>
////      (Point(p.getX.toFloat, p.getY.toFloat), "1"))).cache()
//
//    val rightRDD = queryRDD.rawSpatialRDD.rdd
//      .map(p => Point(p.getX.toFloat, p.getY.toFloat))
//      .cache()
//
//    val leftpoints = queryRDD.rawSpatialRDD.sparkContext.textFile("/home/vontman/Downloads/points_10M_wkt.csv").map(x => (Try(new WKTReader().read(x))))
//      .filter(_.isSuccess).map {
//      case x =>
//        val corrds = x.get.getCoordinates
//        val p1 = corrds(0)
//        (Point(p1.x.toFloat, p1.y.toFloat), "1")
//    }
//
//    val leftRDD = SpatialRDD(leftpoints).cache()
//
//    println("leftRDD Count: " + leftRDD.count())
//    println("rightRDD Count: " + rightRDD.count())
//
//    val t0 = System.currentTimeMillis()
//
//
//    val result =
//      leftRDD.knnjoin(rightRDD, k, _ => true, _ => true)
//
//    println(s"Time Elapsed: ${System.currentTimeMillis() - t0}")
//
//    val geometryFactory = new GeometryFactory()
//    JavaPairRDD.fromRDD(
//      result
//        .map{case (k, v) => (geometryFactory.createPoint(new geom.Coordinate(k.x, k.y)), v)}
//        .flatMapValues(
//        _.map(
//          x =>
//            geometryFactory.createPoint(
//              new Coordinate(
//                x._1.x,
//                x._1.y
//              )
//          )
//        )
//      )
//    )
//  }
//}
