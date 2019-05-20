package knnjoin;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;

public class KNNJoinWithCircles implements KNNJoinSolver {

    private ReduceKNNLogic reduceKNNLogic;

    public KNNJoinWithCircles() {
        this.reduceKNNLogic = ReduceKNNLogic.REDUCE_BY_KEY;
    }

    public KNNJoinWithCircles(ReduceKNNLogic reduceKnnLogic) {
        this.reduceKNNLogic = reduceKnnLogic;
    }

    static private void setUpIndex(SpatialRDD<Point> dataRDD, SpatialRDD<Point> queryRDD)
        throws Exception {
        dataRDD.boundaryEnvelope.expandToInclude(queryRDD.boundaryEnvelope);

        dataRDD.spatialPartitioning(GridType.QUADTREE);
        dataRDD.buildIndex(IndexType.RTREE, true);
        queryRDD.spatialPartitioning(dataRDD.getPartitioner());
        queryRDD.buildIndex(IndexType.RTREE, true);

        dataRDD.indexedRDD.cache();
        queryRDD.indexedRDD.cache();
    }

    static private List<Tuple2<Envelope, Integer>> computeBoundsAndSizes(
        SpatialRDD<Point> dataRDD) {
        JavaRDD<Tuple2<Envelope, Integer>> x = dataRDD.indexedRDD
            .mapPartitions((Iterator<SpatialIndex> indices) -> {
                List<Tuple2<Envelope, Integer>> ret = new ArrayList<>();

                indices.forEachRemaining(
                    index -> {
                        final STRtree rtree = (STRtree) index;
                        Envelope bounds = (Envelope) rtree.getRoot().getBounds();
                        ret.add(new Tuple2<>(bounds, rtree.size()));
                    }
                );
                return ret.iterator();
            }, true).filter(tuple -> tuple._1 != null);

        return new ArrayList<>(x.collect());
    }

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

    static private CircleRDD computeKnnCirclesFromEachPartition(SpatialRDD<Point> dataRDD,
        SpatialRDD<Point> queryRDD, List<Tuple2<Envelope, Integer>> boundsAndSizes, int k) {
        JavaRDD<Circle> knnCirclesRDD = queryRDD.spatialPartitionedRDD
            .zipPartitions(dataRDD.indexedRDD,
                (Iterator<Point> points, Iterator<SpatialIndex> indices) -> {
                    List<Circle> circles = new ArrayList<>();
                    STRtree index = (STRtree) indices.next();
                    final int pointsCount = index.size();
                    final Envelope bounds = (Envelope) index.getRoot().getBounds();

                    if ( bounds != null && pointsCount >= k ) {
                        points.forEachRemaining(point -> {

                            Object[] knn =
                                index.kNearestNeighbour(point.getEnvelopeInternal(), point,
                                    new GeometryItemDistance(), k);
                            double furthestDistance = 0;
                            for ( Object p : knn ) {
                                furthestDistance = Math
                                    .max(furthestDistance, point.distance((Point) p));
                            }
                            circles.add(new Circle(point, furthestDistance));
                        });
                    } else {
                        points.forEachRemaining(point -> {
                            boundsAndSizes.sort(new PartitionComparator(point));
                            int pointIncluded = 0;
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

        return new CircleRDD(knnCirclesRDD);
    }

    static private JavaPairRDD<Point, Tuple2<Point, List<Point>>> computeKnnAfterReplication(
        CircleRDD knnCirclesRDD, SpatialRDD<Point> dataRDD, int k) {

        knnCirclesRDD.spatialPartitioning(dataRDD.getPartitioner());

        return JavaPairRDD.fromJavaRDD(
            knnCirclesRDD.spatialPartitionedRDD
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
                                knn.sort(Comparator.comparingDouble(center::distance));

                                results.add(Tuple2.apply(center, Tuple2.apply(center, knn)));
                            });
                        }
                        return results.iterator();
                    }));
    }

    static private List<Point> reducePointsForCenter(int k, Point center, List<List<Point>> pointsLists) {
        List<Point> knn = new ArrayList<>(k);

        List<Integer> indices = new ArrayList<>(pointsLists.size());

        pointsLists.forEach(l -> indices.add(0));

        while ( knn.size() < k ) {
            int chosenList = -1;
            double bestDistance = 0.0;

            for (int index = 0; index < pointsLists.size(); index++) {
                final List<Point> points = pointsLists.get(index);
                final int i = indices.get(index);

                if (i >= points.size())
                    continue;

                final double distance = center.distance(points.get(i));
                if (chosenList == -1 || bestDistance > distance) {
                    chosenList = index;
                    bestDistance = distance;
                }
            }

            if (chosenList == -1)
                break;

            knn.add(pointsLists.get(chosenList).get(indices.get(chosenList)));
            indices.set(chosenList, indices.get(chosenList) + 1);
        }

        return knn;
    }

    static private JavaPairRDD<Point, List<Point>> reduceResultByReduceByKey(int k,
        JavaPairRDD<Point, Tuple2<Point, List<Point>>> resRDD) {

        return resRDD
            .reduceByKey((t1, t2) -> {
                final Point center = t1._1;
                final List<Point> points_1 = t1._2;
                final List<Point> points_2 = t2._2;

                return Tuple2.apply(center, reducePointsForCenter(k, center, Arrays.asList(points_1, points_2)));
            }).mapValues(t -> t._2);
    }

    static private JavaPairRDD<Point, List<Point>> reduceResultByGroupByKey(int k,
        JavaPairRDD<Point, Tuple2<Point, List<Point>>> resRDD) {

        return resRDD
            .groupByKey()
            .mapValues((Iterable<Tuple2<Point, List<Point>>> tuples) -> {

                Point center = null;
                List<List<Point>> pointsLists = new ArrayList<>();
                for ( Tuple2<Point, List<Point>> t : tuples ) {
                    center = t._1;
                    final List<Point> points = t._2;
                    pointsLists.add(points);
                }

                if (center == null) {
                    return Collections.emptyList();
                }

                return reducePointsForCenter(k, center, pointsLists);
            });
    }

    @Override
    public JavaPairRDD<Point, List<Point>> solve(GeometryFactory geometryFactory,
        SpatialRDD<Point> dataRDD,
        SpatialRDD<Point> queryRDD,
        int k,
        StringBuilder resultStr, boolean visualize, String outputPath) throws Exception {

        setUpIndex(dataRDD, queryRDD);

        List<Tuple2<Envelope, Integer>> boundsAndSizes = computeBoundsAndSizes(dataRDD);

        final CircleRDD knnCirclesFromEachPartition = computeKnnCirclesFromEachPartition(
            dataRDD, queryRDD, boundsAndSizes, k);

        final JavaPairRDD<Point, Tuple2<Point, List<Point>>> resultsRDD = computeKnnAfterReplication(
            knnCirclesFromEachPartition, dataRDD, k);

        if ( reduceKNNLogic == ReduceKNNLogic.REDUCE_BY_KEY ) {
            return reduceResultByReduceByKey(k, resultsRDD).cache();
        }
        if ( reduceKNNLogic == ReduceKNNLogic.GROUP_BY_KEY ) {
            return reduceResultByGroupByKey(k, resultsRDD).cache();
        }
        return null;
    }

    enum ReduceKNNLogic {
        REDUCE_BY_KEY,
        GROUP_BY_KEY
    }

    static class PartitionComparator implements Comparator<Tuple2<Envelope, Integer>> {

        private Point point;

        PartitionComparator(Point point) {
            this.point = point;
        }

        @Override
        public int compare(Tuple2<Envelope, Integer> o1, Tuple2<Envelope, Integer> o2) {
            Envelope env1 = o1._1;
            Envelope env2 = o2._1;
            Coordinate furthestVertex1 = getFurthestVertex(env1, point);
            Coordinate furthestVertex2 = getFurthestVertex(env2, point);
            return Double.compare(point.getCoordinate().distance(furthestVertex1),
                point.getCoordinate().distance(furthestVertex2));
        }
    }
}

