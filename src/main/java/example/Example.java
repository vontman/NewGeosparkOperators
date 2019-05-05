package example;


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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class Example {

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        deleteOldValidation();

        PointRDD data = new GenerateGuassianData().generate(101000, 800000, sc.sc());

        data.analyze();
        data.spatialPartitioning(GridType.QUADTREE);
        data.buildIndex(IndexType.QUADTREE, true);
        data.indexedRDD.cache();

        Plotter.visualizeQ(sc, data, "OriginalData", true);

        PointRDD nextRdd = data;
        long prevCount, nextCount;
        int pruningIteration = 0;
//        do {
        List<Integer> levels = Arrays.asList(6, 7, 8);
        for (Integer level : levels) {
            prevCount = data.countWithoutDuplicates();
            System.out.println("Before # of Points = " + prevCount);
            nextRdd = OutliersDetectionQ.findOutliers(data, 300, 300, level, level);
            nextCount = nextRdd.countWithoutDuplicates();
            System.out.println("After # of Points = " + nextCount + "\n");
            System.out.println("Pruning = " + ((1.0 * prevCount - nextCount) / prevCount * 100.0) + "\n");
            if (prevCount != nextCount)
                Plotter.visualizeQ(sc, nextRdd, "FilteredData_" + level, true);
        }

//        } while (nextCount < prevCount);

//        runNaiiveSolution(sc, data, nextRdd);

        sc.stop();
    }

    private static void runNaiiveSolution(JavaSparkContext sc, PointRDD data, PointRDD nextRdd) throws Exception {
        data.spatialPartitioning(GridType.QUADTREE);
        data.buildIndex(IndexType.QUADTREE, true);
        data.indexedRDD.cache();

        int found = 0;
        List<Point> doubtList = nextRdd.spatialPartitionedRDD.collect();
        List<Point> ans = OutliersDetectionNaiive.findOutliersNaive(data, 100, 100);
        for (Point p : ans) {
            for (Point x : doubtList) {
                if (x.getX() == p.getX() && x.getY() == p.getY()) {
                    found++;
                }
            }
        }
        System.out.println(ans.size() == found ? "VALID SOLUTION" : "INVALID SOLUTION");
        PointRDD solutionRdd = new PointRDD(sc.parallelize(ans));

        solutionRdd.analyze();

        Plotter.visualizeNaiive(sc, solutionRdd, "NaiiveSolution");
        System.out.println(ans.size() + " Outliers were found!");

    }

    private static void deleteOldValidation() throws IOException, InterruptedException {
        System.out.println("Delete old visualizations");
        File visualizationsFile = new File("visualization");
        Process process = Runtime.getRuntime().exec(String.format("rm -rf %s", visualizationsFile.getAbsolutePath()));
        process.waitFor();
    }
}