package knnjoin;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
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

    @Override
    public JavaPairRDD<Point, Point> solve(GeometryFactory geometryFactory,
        SpatialRDD<Point> dataRDD,
        SpatialRDD<Point> queryRDD,
        int k,
        StringBuilder resultStr, boolean visualize, String outputPath) throws Exception {

        dataRDD.spatialPartitioning(GridType.QUADTREE);
        dataRDD.buildIndex(IndexType.RTREE, true);
        queryRDD.spatialPartitioning(dataRDD.getPartitioner());
        queryRDD.buildIndex(IndexType.RTREE, true);

        final JavaRDD<Circle> resultWithDuplicates;

        resultWithDuplicates =
            queryRDD.spatialPartitionedRDD.zipPartitions(dataRDD.indexedRDD,
                (Iterator<Point> points, Iterator<SpatialIndex> indices) -> {
                    STRtree index = (STRtree) indices.next();
                    List<Circle> circles = new ArrayList<>();
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
                    circles.forEachRemaining(circle -> {
                        final Point center = (Point) circle.getCenterGeometry();

                        Object[] res =
                            index.kNearestNeighbour(center.getEnvelopeInternal(),
                                center,
                                new GeometryItemDistance(), k);
                        for ( Object re : res ) {
                            results.add(Tuple2.apply(center, (Point) re));
                        }
                    });
                    return results.iterator();
                }).mapToPair(p -> Tuple2.apply(p._1, p));

        JavaPairRDD<Point, Point> pointTuple2JavaPairRDD = resultsRdd
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
                        if ( pq.size() >= k ) {
                            double d1 = p._1.distance(p._2);
                            double d2 = pq.peek()._1.distance(pq.peek()._2);
                            if ( d2 > d1 ) {
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

        return pointTuple2JavaPairRDD.cache();
    }
}
