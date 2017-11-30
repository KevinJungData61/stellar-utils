package sh.serene.sereneutils.model.epgm;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * EPGM Graph collection representation as spark datasets
 */
public class GraphCollection {

    /**
     * EPGM Graph Heads
     */
    private Dataset<GraphHead> graphHeads;

    /**
     * EPGM Vertices
     */
    private Dataset<VertexCollection> vertices;

    /**
     * EPGM Edges
     */
    private Dataset<EdgeCollection> edges;

    public GraphCollection() { }

    public GraphCollection(Dataset<GraphHead> graphHeads, Dataset<VertexCollection> vertices, Dataset<EdgeCollection> edges) {
        this.graphHeads = graphHeads;
        this.vertices = vertices;
        this.edges = edges;
    }

    /**
     * Creates an EPGM Graph Collection from datasets
     *
     * @param graphHeads    graph head dataset
     * @param vertices      vertex dataset
     * @param edges         edge dataset
     * @return              graph collection
     */
    public static GraphCollection fromDatasets(Dataset<GraphHead> graphHeads, Dataset<VertexCollection> vertices, Dataset<EdgeCollection> edges) {
        return new GraphCollection(graphHeads, vertices, edges);
    }

    public Dataset<GraphHead> getGraphHeads() {
        return this.graphHeads;
    }

    public Dataset<VertexCollection> getVertices() {
        return this.vertices;
    }

    public Dataset<EdgeCollection> getEdges() {
        return this.edges;
    }

    private byte[] concatenateIds(ElementId id1, ElementId id2) {
        byte[] bytes = new byte[id1.getBytes().length + id2.getBytes().length];
        System.arraycopy(id1.getBytes(), 0, bytes, 0, id1.getBytes().length);
        System.arraycopy(id2.getBytes(), 0, bytes, id1.getBytes().length, id2.getBytes().length);
        return bytes;
    }

    private <T extends Element> Dataset<Tuple2<byte[],T>> elemToTuples(
            Dataset<T> elements,
            Class<T> type
    ) {
        return elements.map(
                (MapFunction<T, Tuple2<byte[],T>>) elem ->
                        new Tuple2<>(concatenateIds(elem.getId(), elem.getGraphs().get(0)), elem),
                Encoders.tuple(
                        Encoders.BINARY(),
                        Encoders.bean(type)
                )
        );
    }

    public Dataset<VertexCollection> joinVertexCollections(
            Dataset<VertexCollection> vertices1,
            Dataset<VertexCollection> vertices2
    ) {
        Dataset<Tuple2<byte[],VertexCollection>> tup1 = elemToTuples(vertices1, VertexCollection.class);
        Dataset<Tuple2<byte[],VertexCollection>> tup2 = elemToTuples(vertices2, VertexCollection.class);
        tup1.show();
        return tup1
                .joinWith(
                        tup2,
                        tup1.col("value").equalTo(tup2.col("value")),
                        "fullouter")
                .map((MapFunction<
                        Tuple2<Tuple2<byte[],VertexCollection>,
                                Tuple2<byte[],VertexCollection>>,
                        VertexCollection>) tup -> {
                    if (tup._1 == null) {
                        return tup._2._2;
                    } else if (tup._2 == null) {
                        return tup._1._2;
                    } else {
                        VertexCollection v1 = tup._1._2;
                        VertexCollection v2 = tup._2._2;
                        List<ElementId> graphs = new ArrayList<>(v1.getGraphs());
                        for (ElementId graphId : v2.getGraphs()) {
                            if (!graphs.contains(graphId)) {
                                graphs.add(graphId);
                            }
                        }
                        return VertexCollection.create(
                                v1.getId(),
                                v1.getProperties(),
                                v1.getLabel(),
                                graphs
                        );
                    }
                }, Encoders.bean(VertexCollection.class));
    }

    public Dataset<EdgeCollection> joinEdgeCollections(
            Dataset<EdgeCollection> edges1,
            Dataset<EdgeCollection> edges2
    ) {
        Dataset<Tuple2<byte[],EdgeCollection>> tup1 = elemToTuples(edges1, EdgeCollection.class);
        Dataset<Tuple2<byte[],EdgeCollection>> tup2 = elemToTuples(edges2, EdgeCollection.class);
        return tup1.
                joinWith(
                        tup2,
                        tup1.col("value").equalTo(tup2.col("value")),
                        "fullouter")
                .map((MapFunction<
                        Tuple2<Tuple2<byte[],EdgeCollection>,
                                Tuple2<byte[],EdgeCollection>>,
                        EdgeCollection>) tup -> {
                    if (tup._1 == null) {
                        return tup._2._2;
                    } else if (tup._2 == null) {
                        return tup._1._2;
                    } else {
                        EdgeCollection e1 = tup._1._2;
                        EdgeCollection e2 = tup._2._2;
                        List<ElementId> graphs = new ArrayList<>(e1.getGraphs());
                        for (ElementId graphId : e2.getGraphs()) {
                            if (!graphs.contains(graphId)) {
                                graphs.add(graphId);
                            }
                        }
                        return EdgeCollection.create(
                                e1.getId(),
                                e1.getSrc(),
                                e1.getDst(),
                                e1.getProperties(),
                                e1.getLabel(),
                                graphs
                        );
                    }
                }, Encoders.bean(EdgeCollection.class));
    }
}
