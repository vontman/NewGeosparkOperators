package knnjoin;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;

@SuppressWarnings("Duplicates")
public class KNNJoinWithCirclesWithReduceByKey implements KNNJoinSolver {

    static private Coordinate[] envToCoordinate(Envelope env) {
        return new Coordinate[]{new Coordinate(env.getMinX(), env.getMinY()),
            new Coordinate(env.getMinX(), env.getMaxY()),
            new Coordinate(env.getMaxX(), env.getMaxY()),
            new Coordinate(env.getMaxX(), env.getMinY())};
    }

    static private Coordinate getFurthestVertex(Envelope env, Point point) {
        Coordinate[] vertices = envToCoordinate(env);
        Arrays.sort(vertices,
            Comparator.comparingDouble(a -> point.getCoordinate().distance((Coordinate) a))
                .reversed());
        return vertices[0];
    }

    static private List<Point> reducePointsForCenter(int k, Point center, Iterator<Point> points) {

        PriorityQueue<Point> pq = new PriorityQueue<>(
            (a, b) -> {
                double d1 = center.distance(a);
                double d2 = center.distance(b);
                return Double.compare(d2, d1);
            }
        );

        points.forEachRemaining(point -> {
            if ( pq.size() >= k ) {
                if ( pq.comparator().compare(pq.peek(), point) < 0 ) {
                    pq.poll();
                    pq.add(point);
                }
            } else {
                pq.add(point);
            }
        });

        return new ArrayList<>(pq);
    }

    static private JavaPairRDD<Point, List<Point>> reduceResultByReduceByKey(int k,
        JavaPairRDD<Point, Tuple2<Point, List<Point>>> resRDD) {

        return resRDD
            .reduceByKey((t1, t2) -> {
                final Point center = t1._1;
                final List<Point> points_1 = t1._2;
                final List<Point> points_2 = t2._2;
                final Iterator<Point> pointsIterator = Iterators.concat(points_1.iterator(), points_2.iterator());

                return Tuple2.apply(center, reducePointsForCenter(k, center, pointsIterator));
            }).mapValues(t -> t._2);
    }

    @Override
    public JavaPairRDD<Point, List<Point>> solve(GeometryFactory geometryFactory,
        SpatialRDD<Point> dataRDD,
        SpatialRDD<Point> queryRDD,
        int k,
        StringBuilder resultStr, boolean visualize, String outputPath) throws Exception {

        dataRDD.boundaryEnvelope.expandToInclude(queryRDD.boundaryEnvelope);

        dataRDD.spatialPartitioning(GridType.QUADTREE);
        dataRDD.buildIndex(IndexType.RTREE, true);
        queryRDD.spatialPartitioning(dataRDD.getPartitioner());
        queryRDD.buildIndex(IndexType.RTREE, true);

        queryRDD.indexedRDD.cache();

        JavaPairRDD x = dataRDD.indexedRDD.mapPartitionsToPair((Iterator<SpatialIndex> indices) -> {
            STRtree index = (STRtree) indices.next();
            Envelope bounds = (Envelope) index.getRoot().getBounds();
            List<Tuple2<Envelope, Integer>> ret = new ArrayList<>();
            ret.add(new Tuple2<>(bounds, (index).size()));
            return ret.iterator();
        }).filter(tuple -> tuple._1 != null);

        ArrayList<Tuple2<Envelope, Integer>> boundsAndSizes = new ArrayList<>(x.collect());

//        System.out.println(boundsAndSizes.size());

        final JavaRDD<Circle> resultWithDuplicates;
        resultWithDuplicates =
            queryRDD.spatialPartitionedRDD.zipPartitions(dataRDD.indexedRDD,
                (Iterator<Point> points, Iterator<SpatialIndex> indices) -> {
                    List<Circle> circles = new ArrayList<>();
                    STRtree index = (STRtree) indices.next();
                    final int pointsCount = index.size();

                    if ( index.getRoot().getBounds() != null && pointsCount >= k ) {
                        points.forEachRemaining(point -> {

                            Object[] res =
                                index.kNearestNeighbour(point.getEnvelopeInternal(), point,
                                    new GeometryItemDistance(), k);
                            double furthestDistance = 0;
                            for ( Object re : res ) {
                                furthestDistance = Math
                                    .max(furthestDistance, point.distance((Point) re));
                            }
                            circles.add(new Circle(point, furthestDistance));
                        });
                    } else {
                        points.forEachRemaining(point -> {
                            boundsAndSizes.sort(new PartitionComparator(point));
                            int pointIncluded = pointsCount;
                            Coordinate chosenVertex = null;
                            for ( Tuple2<Envelope, Integer> boundsAndSize : boundsAndSizes ) {
                                pointIncluded += boundsAndSize._2;
                                chosenVertex = getFurthestVertex(boundsAndSize._1, point);
                                if ( pointIncluded >= k ) {
                                    break;
                                }
                            }

                            circles.add(
                                new Circle(point, point.getCoordinate().distance(chosenVertex)));
                        });

                    }
                    return circles.iterator();

                });

        final CircleRDD circleRDD = new CircleRDD(resultWithDuplicates);
        circleRDD.spatialPartitioning(dataRDD.getPartitioner());

        JavaPairRDD<Point, Tuple2<Point, List<Point>>> resultsRdd = JavaPairRDD.fromJavaRDD(
            circleRDD.spatialPartitionedRDD
                .zipPartitions(dataRDD.indexedRDD,
                    (Iterator<Circle> circles, Iterator<SpatialIndex> indices) -> {
                        STRtree index = (STRtree) indices.next();
                        final List<Tuple2<Point, Tuple2<Point, List<Point>>>> results = new ArrayList<>();

                        if ( index.getRoot().getBounds() != null ) {
                            circles.forEachRemaining(circle -> {
                                final Point center = (Point) circle.getCenterGeometry();
                                List<Point> knn = new ArrayList<>();

                                Object[] res =
                                    index.kNearestNeighbour(center.getEnvelopeInternal(),
                                        center,
                                        new GeometryItemDistance(), k);
                                for ( Object re : res ) {
                                    knn.add((Point) re);
                                }
                                results.add(Tuple2.apply(center, Tuple2.apply(center, knn)));
                            });
                        }
                        return results.iterator();
                    }));

        return reduceResultByReduceByKey(k, resultsRdd).cache();
    }

    static class PartitionComparator implements Comparator<Tuple2<Envelope, Integer>> {

        private Point point;


        public PartitionComparator(Point point) {
            this.point = point;
        }

        @Override
        public int compare(Tuple2<Envelope, Integer> o1, Tuple2<Envelope, Integer> o2) {
            Envelope env1 = o1._1;
            Envelope env2 = o2._1;
            Coordinate furthestVirtex1 = getFurthestVertex(env1, point);
            Coordinate furthestVirtex2 = getFurthestVertex(env2, point);
            return Double.compare(point.getCoordinate().distance(furthestVirtex1),
                point.getCoordinate().distance(furthestVirtex2));
        }
    }
}

