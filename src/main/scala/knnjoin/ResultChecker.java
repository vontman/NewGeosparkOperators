package knnjoin;

import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ResultChecker {

    static JavaPairRDD<Point, List<Double>> transformToDist(JavaPairRDD<Point, Point> a){
        JavaPairRDD<Point, Iterable<Point>> pointToList = a.groupByKey();
        return pointToList.mapToPair(t -> {
            List<Double> d = new ArrayList<>();
            for (Point p : t._2) {
                d.add(p.distance(t._1));
            }
            Collections.sort(d);
            return new Tuple2<>(t._1, d);
        });
    }

    static String compare(JavaPairRDD<Point, Point> a, JavaPairRDD<Point, Point> b, int k){
        List<Tuple2<Point, List<Double>>> c = transformToDist(a).sortByKey().collect();
        List<Tuple2<Point, List<Double>>> d = transformToDist(b).sortByKey().collect();
        if(c.size() != d.size()) return "rdd size mismatch";
        for(int i = 0; i < c.size(); i++) {
            if (!c.get(i)._1.equals(d.get(i)._1)) {
                System.out.println(c.get(i)._1.toString());
                System.out.println(d.get(i)._1.toString());

                return "key mismatch";
            }


            if (c.get(i)._2.size() != k || d.get(i)._2.size() != k) {
                System.out.println(c.get(i)._2.size());
                System.out.println(d.get(i)._2.size());
                return "wrong k";
            }

            boolean dist_mismatch = false;
            for (int j = 0; j < k; j++) {
                if (!c.get(i)._2.get(j).equals(d.get(i)._2.get(j))) {
                    dist_mismatch = true;
                }

            }
            if(dist_mismatch) {
                System.out.print(c.get(i)._1.toString()+": ");

                System.out.println(c.get(i)._2.toString());
                System.out.print(d.get(i)._1.toString()+": ");

                System.out.println(d.get(i)._2.toString());
                return "distance mismatch";
            }
        }

        return "OK";
    }
}
