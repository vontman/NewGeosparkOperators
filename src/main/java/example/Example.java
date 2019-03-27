package example;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class Example implements Serializable {

    /**
     * The geometry factory.
     */
    private static GeometryFactory geometryFactory;

    /**
     * The Point RDD input location.
     */
    static String PointRDDInputLocation;

    /**
     * The Point RDD splitter.
     */
    static FileDataSplitter PointRDDSplitter;


    private static List<Point> generateRandomInput(int n, int range) {
        Random rand = new Random();
        List<Point> input = IntStream.range(0, n).mapToObj((x) ->
                geometryFactory.createPoint(
                        new Coordinate(rand.nextInt(range), rand.nextInt(range))))
                .collect(Collectors.toList());
        return input;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        geometryFactory = new GeometryFactory();
        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        /**
         * The sc.
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Point> points = generateRandomInput(1000000, 1000);
        PointRDD data = new PointRDD(sc.parallelize(points, 10));

        data.analyze();
        data.spatialPartitioning(GridType.RTREE);
        data.buildIndex(IndexType.RTREE, true);
        data.indexedRDD.cache();

        Plotter.visualize(sc, data, "OriginalData");

        PointRDD nextRdd = data;

        for (int i = 0; i < 2 && nextRdd.approximateTotalCount > 40; i++) {
            nextRdd = OutliersDetection.findOutliers(nextRdd, 100, 100);
            Plotter.visualize(sc, nextRdd, "FilteredData_" + i);
        }

        int found = 0;
        List<Point> doubtList = nextRdd.spatialPartitionedRDD.collect();
        List<Point> ans = OutliersDetection.findOutliersNaive(data, 50, 20);
        for (Point p : ans) {
            for (Point x : doubtList) {
                if (x.getX() == p.getX() && x.getY() == p.getY()) {
                    found++;
                }
            }
        }
        System.out.println(ans.size() == found ? "VALID SOLUTION" : "INVALID SOLUTION");


        sc.stop();
    }

}