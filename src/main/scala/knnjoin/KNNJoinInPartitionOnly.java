package knnjoin;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;

public class KNNJoinInPartitionOnly implements KNNJoinSolver {

    @Override
    public JavaPairRDD<Point, List<Point>> solve(GeometryFactory geometryFactory,
        SpatialRDD<Point> dataRDD,
        SpatialRDD<Point> queryRDD,
        int k,
        StringBuilder resultStr, boolean visualize, String outputPath) throws Exception {

        dataRDD.spatialPartitioning(GridType.QUADTREE);
        dataRDD.buildIndex(IndexType.RTREE, true);
        queryRDD.spatialPartitioning(dataRDD.getPartitioner());
        queryRDD.buildIndex(IndexType.RTREE, true);

        final JavaPairRDD<Point, List<Point>> resultWithDuplicates;

        resultWithDuplicates =
            JavaPairRDD.fromJavaRDD(
                queryRDD.spatialPartitionedRDD.zipPartitions(dataRDD.indexedRDD,
                    (Iterator<Point> points, Iterator<SpatialIndex> indices) -> {
                        STRtree index = (STRtree) indices.next();
                        List<Tuple2<Point, List<Point>>> result = new ArrayList<>();
                        points.forEachRemaining(point -> {
                            List<Point> knn = new ArrayList<>();
                            if ( index.getRoot().getBounds() != null ) {

                                Object[] res =
                                    index.kNearestNeighbour(point.getEnvelopeInternal(), point,
                                        new GeometryItemDistance(), k);
                                for ( Object re : res ) {
                                    knn.add((Point) re);
                                }
                            }
                            result.add(Tuple2.apply(point, knn));
                        });
                        return result.iterator();
                    })
            );

        return resultWithDuplicates.cache();
    }
}
