package outliersdetection

import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

trait LevelExpander {
  val parametersList: List[_]

  def expand(inputRDD: PointRDD): RDD[IndexNode]
}
