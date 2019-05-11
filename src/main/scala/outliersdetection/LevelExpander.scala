package outliersdetection

import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import utils.IndexNode

trait LevelExpander extends Serializable {
  def expand(inputRDD: PointRDD): RDD[IndexNode]
}
