package knnjoin;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class KNNJoinWithCircles implements KNNJoinSolver {

    static private Coordinate[] envToCoordinate(Envelope env) {
        return new Coordinate[]{new Coordinate(env.getMinX(), env.getMinY()),
                new Coordinate(env.getMinX(), env.getMaxY()),
                new Coordinate(env.getMaxX(), env.getMaxY()),
                new Coordinate(env.getMaxX(), env.getMinY())};
    }

    static private Coordinate getFurthestVertex(Envelope env, Point point) {
        Coordinate[] vertices = envToCoordinate(env);
        Arrays.sort(vertices,
                Comparator.comparingDouble(a -> point.getCoordinate().distance((Coordinate) a)).reversed());
        return vertices[0];
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


    @Override
    public JavaPairRDD<Point, Point> solve(
            PointRDD dataRDD,
            PointRDD queryRDD,
            int k) throws Exception {

//        dataRDD.boundaryEnvelope.expandToInclude(queryRDD.boundaryEnvelope);

        dataRDD.spatialPartitioning(GridType.QUADTREE);
        dataRDD.buildIndex(IndexType.RTREE, true);
        queryRDD.spatialPartitioning(dataRDD.getPartitioner());
        queryRDD.buildIndex(IndexType.RTREE, true);

        ArrayList<Tuple2<Envelope, Integer>> boundsAndSizes = new ArrayList<>(
                dataRDD.indexedRDD.mapToPair(spatialIndex -> {
                    Envelope bounds = (Envelope) ((STRtree) spatialIndex).getRoot().getBounds();
                    return new Tuple2<>(bounds, ((STRtree) spatialIndex).getRoot().pointsCount());
                }).filter(tuple -> tuple._1 != null).collect());


        final JavaRDD<Circle> resultWithDuplicates;
        resultWithDuplicates =
                queryRDD.spatialPartitionedRDD.zipPartitions(dataRDD.indexedRDD,
                        (Iterator<Point> points, Iterator<SpatialIndex> indices) -> {
                            List<Circle> circles = new ArrayList<>();
                            STRtree index = (STRtree) indices.next();
//                    System.out.print("ROOT: ");
//                    System.out.println(index.getRoot().getBounds());

                            if (index.getRoot().getBounds() != null) {
                                points.forEachRemaining(point -> {

                                    Object[] res =
                                            index.kNearestNeighbour(point.getEnvelopeInternal(), point,
                                                    new GeometryItemDistance(), k);
                                    double furthestDistance = 0;
                                    for (Object re : res) {
                                        furthestDistance = Math
                                                .max(furthestDistance, point.distance((Point) re));
                                    }
                                    circles.add(new Circle(point, furthestDistance));
                                });
                            } else {
                                points.forEachRemaining(point -> {
                                    boundsAndSizes.sort(new PartitionComparator(point));
                                    int pointIncluded = 0;
                                    Coordinate chosenVertex = null;
                                    for (Tuple2<Envelope, Integer> boundsAndSize : boundsAndSizes) {
                                        pointIncluded += boundsAndSize._2;
                                        chosenVertex = getFurthestVertex(boundsAndSize._1, point);
                                        if (pointIncluded >= k) {
                                            break;
                                        }
                                    }
                                    circles.add(new Circle(point, point.getCoordinate().distance(chosenVertex)));
                                });
                            }
                            return circles.iterator();

                        });

        final CircleRDD circleRDD = new CircleRDD(resultWithDuplicates);
        circleRDD.spatialPartitioning(dataRDD.getPartitioner());

        /*
        final PolygonRDD boundsRDD = new PolygonRDD(

            dataRDD.indexedRDD.map(index -> ((STRtree) index).getRoot().getBounds())
                .map(bounds -> (Envelope) bounds).map(
                bounds -> {

                    Coordinate[] coordinates = {
                        new Coordinate(bounds.getMinX(), bounds.getMinY()),
                        new Coordinate(bounds.getMinX(), bounds.getMaxY()),
                        new Coordinate(bounds.getMaxX(), bounds.getMaxY()),
                        new Coordinate(bounds.getMaxX(), bounds.getMinY()),
                        new Coordinate(bounds.getMinX(), bounds.getMinY())
                    };
                    return geometryFactory.createPolygon(coordinates);
                }
            )
        );
        boundsRDD.analyze();

        Visualization
            .buildScatterPlot(
                JavaConverters.collectionAsScalaIterableConverter(Arrays.asList(circleRDD, dataRDD, boundsRDD)).asScala(),
                outputPath + "_circles");
        */

        JavaPairRDD<Point, Tuple2<Point, Point>> resultsRdd = circleRDD.spatialPartitionedRDD
                .zipPartitions(dataRDD.indexedRDD,
                        (Iterator<Circle> circles, Iterator<SpatialIndex> indices) -> {
                            STRtree index = (STRtree) indices.next();
                            final List<Tuple2<Point, Point>> results = new ArrayList<>();

                            if (index.getRoot().getBounds() != null) {
                                circles.forEachRemaining(circle -> {
                                    final Point center = (Point) circle.getCenterGeometry();

                                    Object[] res =
                                            index.kNearestNeighbour(center.getEnvelopeInternal(),
                                                    center,
                                                    new GeometryItemDistance(), k);
                                    for (Object re : res) {
                                        results.add(Tuple2.apply(center, (Point) re));
                                    }
                                });
                            }
                            return results.iterator();
                        }).mapToPair(p -> Tuple2.apply(p._1, p));

        return resultsRdd
                .groupByKey().flatMapValues(
                        (Iterable<Tuple2<Point, Point>> points) -> {
                            PriorityQueue<Tuple2<Point, Point>> pq = new PriorityQueue<>(
                                    (a, b) -> {
                                        double d1 = a._1.distance(a._2);
                                        double d2 = b._1.distance(b._2);
                                        return Double.compare(d2, d1);
                                    }
                            );
                            points.forEach(p -> {
                                if (pq.size() >= k) {
                                    double d1 = p._1.distance(p._2);
                                    double d2 = pq.peek()._1.distance(pq.peek()._2);
                                    if (d2 > d1) {
                                        pq.poll();
                                        pq.add(p);
                                    }
                                } else {
                                    pq.add(p);
                                }
                            });

                            return pq.stream().map(p -> p._2).collect(Collectors.toList());
                        }
                );
    }
}
