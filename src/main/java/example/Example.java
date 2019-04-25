package example;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class Example {

    /**
     * The geometry factory.
     */
    private static GeometryFactory geometryFactory;


    private static List<Point> generateRandomInput() {
        Random rand = new Random();
        return IntStream.range(0, 1000).mapToObj((x) ->
                geometryFactory.createPoint(
                        new Coordinate(
                                rand.nextGaussian() * rand.nextInt(500),
                                rand.nextGaussian() * rand.nextInt(500)
                        )))
                .collect(Collectors.toList());

    }

    private static List<Point> generateLolData() {
        Random rand = new Random();
        List<Point> lol1 = IntStream.range(0, 100000).mapToObj((x) ->
                geometryFactory.createPoint(
                        new Coordinate(rand.nextInt(10000), rand.nextInt(10000))))
                .collect(Collectors.toList());
//        List<Point> lol2 = IntStream.range(0, 1000).mapToObj((x) ->
//                geometryFactory.createPoint(
//                        new Coordinate(200 + rand.nextInt(200), 200 + rand.nextInt(200))))
//                .collect(Collectors.toList());
//
//        lol1.addAll(lol2);
        return lol1;
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

        JavaSparkContext sc = new JavaSparkContext(conf);


//        List<Point> points = generateRandomInput();
//        PointRDD data = new PointRDD(sc.parallelize(points, 50));

        PointRDD data = new GenerateZipfData(0.8).generate(100000, 800000, sc.sc());

        data.analyze();
        data.spatialPartitioning(GridType.EQUALGRID);
        data.buildIndex(IndexType.QUADTREE, true);
        data.indexedRDD.cache();

        Plotter.visualizeQuad(sc, data, "OriginalData", true);

        PointRDD nextRdd = data;
        long prevCount, nextCount;
        int pruningIteration = 0;
        do {
            prevCount = nextRdd.approximateTotalCount;
            System.out.println("Before # of Points = " + prevCount);
            nextRdd = OutliersDetectionQ.findOutliers(nextRdd, 1000, 500);
            nextCount = nextRdd.approximateTotalCount;
            System.out.println("After # of Points = " + nextCount);
            if (prevCount != nextCount)
                Plotter.visualizeR(sc, nextRdd, "FilteredData_" + pruningIteration++, true);
        } while (nextCount < prevCount);

//        data = new PointRDD(sc.parallelize(points));
//        data.analyze();
//        data.spatialPartitioning(GridType.RTREE);
//        data.buildIndex(IndexType.RTREE, true);
//        data.indexedRDD.cache();
//
//        int found = 0;
//        List<Point> doubtList = nextRdd.spatialPartitionedRDD.collect();
//        List<Point> ans = OutliersDetection.findOutliersNaive(data, 51, 50);
//        for (Point p : ans) {
//            for (Point x : doubtList) {
//                if (x.getX() == p.getX() && x.getY() == p.getY()) {
//                    found++;
//                }
//            }
//        }
//        System.out.println(ans.size() == found ? "VALID SOLUTION" : "INVALID SOLUTION");
//        PointRDD solutionRdd = new PointRDD(sc.parallelize(ans));
//
//        solutionRdd.analyze();
//        solutionRdd.spatialPartitioning(GridType.RTREE);
//        solutionRdd.buildIndex(IndexType.RTREE, true);
//        solutionRdd.indexedRDD.cache();

//        Plotter.visualize(sc, solutionRdd, "NaiiveSolution", true);
//        System.out.println(ans.size() + " Outliers were found!");


        sc.stop();
    }

}