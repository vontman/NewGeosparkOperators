package outliersdetection

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import knnjoin.KNNJoinWithCircles
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConversions._
import scala.language.postfixOps

object OutliersDetectionNaiveWithKNNJoin {
  def findOutliersNaive2(rdd: PointRDD, k: Int, n: Int, originalData: PointRDD): List[Point] = {

    val resRDD = new KNNJoinWithCircles().solve(new GeometryFactory(), originalData, rdd, k, null, false, "")

    println("Executing naive outliers detection")

    resRDD.rdd
      .map({case (p, knn) => (p, knn.map(p.distance).max)})
      .takeOrdered(n)(Ordering.by[(Point, Double), Double](_._2).reverse)
      .map(_._1)
      .toList
  }

  def findOutliersNaive3(rdd: PointRDD, k: Int, n: Int, originalData: PointRDD): List[(Point, Double)] = {

    val resRDD = new KNNJoinWithCircles()
      .solve(new GeometryFactory(), originalData, rdd, k, null, false, "")

    println("Executing naive outliers detection")

    resRDD.rdd
      .map({case (p, knn) => (p, knn.map(p.distance).max)})
      .takeOrdered(n)(Ordering.by[(Point, Double), Double](_._2).reverse)
      .toList

  }

  def findOutliersNaive(rdd: PointRDD, k: Int, n: Int): List[Point] = {

    val resRDD = new KNNJoinWithCircles().solve(new GeometryFactory(), rdd, rdd, k, null, false, "")

    println("Executing naive outliers detection")

    resRDD.rdd
      .map({case (p, knn) => (p, knn.map(p.distance).max)})
      .takeOrdered(n)(Ordering.by[(Point, Double), Double](_._2).reverse)
      .map(_._1)
      .toList
  }
}
