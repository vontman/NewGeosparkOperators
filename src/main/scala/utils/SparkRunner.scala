package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

import scala.util.Random

object SparkRunner {
  def start(): SparkContext = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("GeoSparkRunnableExample")
      .setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator",
      classOf[GeoSparkVizKryoRegistrator].getName)


    println("Starting Spark")
    val sc = new JavaSparkContext(conf)

    println(s"Default Parallelism: ${sc.defaultParallelism}")
    println(s"Default Min Partitions: ${sc.defaultMinPartitions}")

    println(s"Executing a noop operation to initialize the code and executors")

    sc.parallelize((0 until 1000000).map(_ => Random.nextDouble())).reduce(_ * _)

    println(s"DONE")

    sc
  }
}
