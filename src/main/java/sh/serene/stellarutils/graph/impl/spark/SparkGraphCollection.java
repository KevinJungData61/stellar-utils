package sh.serene.stellarutils.graph.impl.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.io.api.StellarWriter;
import sh.serene.stellarutils.io.impl.spark.SparkReader;
import sh.serene.stellarutils.io.impl.spark.SparkWriter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * EPGM Graph collection representation as spark datasets
 */
public class SparkGraphCollection implements StellarGraphCollection, Serializable {

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

    public SparkGraphCollection() { }

    public SparkGraphCollection(
            Dataset<GraphHead> graphHeads,
            Dataset<VertexCollection> vertices,
            Dataset<EdgeCollection> edges
    ) {
        if (graphHeads == null) {
            throw new NullPointerException("Graph Head Dataset was null");
        } else if (vertices == null) {
            throw new NullPointerException("Vertex Collection Dataset was null");
        } else if (edges == null) {
            throw new NullPointerException("Edge Collection Dataset was null");
        }
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
    public static SparkGraphCollection fromDatasets(
            Dataset<GraphHead> graphHeads,
            Dataset<VertexCollection> vertices,
            Dataset<EdgeCollection> edges
    ) {
        return new SparkGraphCollection(graphHeads, vertices, edges);
    }

    /**
     * Creates an EPGM Graph Collection from lists. A spark session with a default configuration is created.
     *
     * @param graphHeads    graph head list
     * @param vertices      vertex list
     * @param edges         edge list
     * @return              graph collection
     */
    public static SparkGraphCollection fromLists(
            List<GraphHead> graphHeads,
            List<VertexCollection> vertices,
            List<EdgeCollection> edges
    ) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Graph Collection")
                .master("local")
                .getOrCreate();
        Dataset<GraphHead> graphHeadDataset = sparkSession.createDataset(graphHeads, Encoders.bean(GraphHead.class));
        Dataset<VertexCollection> vertexDataset = sparkSession
                .createDataset(vertices, Encoders.bean(VertexCollection.class));
        Dataset<EdgeCollection> edgeDataset = sparkSession
                .createDataset(edges, Encoders.bean(EdgeCollection.class));
        return new SparkGraphCollection(graphHeadDataset, vertexDataset, edgeDataset);
    }

    /**
     * Get graph heads
     *
     * @return  graph heads
     */
    public Dataset<GraphHead> getGraphHeads() {
        return this.graphHeads;
    }

    /**
     * Get vertices
     *
     * @return  vertices
     */
    public Dataset<VertexCollection> getVertices() {
        return this.vertices;
    }

    /**
     * Get edges
     *
     * @return  edges
     */
    public Dataset<EdgeCollection> getEdges() {
        return this.edges;
    }

    /**
     * Concatenate two element IDs into a byte array
     *
     * @param id1   element ID 1
     * @param id2   element ID 2
     * @return      byte array
     */
    private byte[] concatenateIds(ElementId id1, ElementId id2) {
        byte[] bytes = new byte[id1.getBytes().length + id2.getBytes().length];
        System.arraycopy(id1.getBytes(), 0, bytes, 0, id1.getBytes().length);
        System.arraycopy(id2.getBytes(), 0, bytes, id1.getBytes().length, id2.getBytes().length);
        return bytes;
    }

    /**
     * Transform element to Tuple of ID|Version (byte array) and element
     *
     * @param elements  elements
     * @param type      class of element
     * @param <T>       vertex or edge
     * @return          tuples
     */
    private <T extends Element> Dataset<Tuple2<byte[],T>> elemToTuples(
            Dataset<T> elements,
            Class<T> type
    ) {
        return elements.map(
                (MapFunction<T, Tuple2<byte[],T>>) elem ->
                        new Tuple2<>(concatenateIds(elem.getId(), elem.version()), elem),
                Encoders.tuple(
                        Encoders.BINARY(),
                        Encoders.bean(type)
                )
        );
    }

    /**
     * Full-outer join vertices with another set of vertices based on ID and version indicated by the graph IDs
     *
     * @param vOther    other vertices
     * @return          new vertices
     */
    public Dataset<VertexCollection> joinVertexCollections(Dataset<VertexCollection> vOther) {
        Dataset<Tuple2<byte[],VertexCollection>> tup1 = elemToTuples(this.vertices, VertexCollection.class);
        Dataset<Tuple2<byte[],VertexCollection>> tup2 = elemToTuples(vOther, VertexCollection.class);
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

    /**
     * Full-outer join edges with another set of edges based on ID and version indicated by the graph IDS.
     *
     * @param eOther    other edges
     * @return          new edges
     */
    public Dataset<EdgeCollection> joinEdgeCollections(Dataset<EdgeCollection> eOther) {
        Dataset<Tuple2<byte[],EdgeCollection>> tup1 = elemToTuples(this.edges, EdgeCollection.class);
        Dataset<Tuple2<byte[],EdgeCollection>> tup2 = elemToTuples(eOther, EdgeCollection.class);
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

    /**
     * Create a reader object with given spark session
     *
     * @param sparkSession  spark session
     * @return              graph collection reader
     */
    public static SparkReader read(SparkSession sparkSession, String path) {
        return new SparkReader(sparkSession, path);
    }

    /**
     * Create a writer object
     *
     * @return  graph collection writer
     */
    @Override
    public StellarWriter write(String path) {
        return new SparkWriter(this, path);
    }

    /**
     * Get graph at index
     *
     * @param index     index
     * @return          graph
     */
    @Override
    public StellarGraph get(int index) {
        return SparkGraph.fromCollection(this, this.graphHeads.toJavaRDD().take(index+1).get(index).getId());
    }

    /**
     * Get graph by ID
     *
     * @param graphId   graph ID
     * @return          graph
     */
    @Override
    public StellarGraph get(ElementId graphId) {
        return SparkGraph.fromCollection(this, graphId);
    }

    /**
     * Union a graph into this graph collection
     *
     * @param graph graph
     * @return new graph collection
     */
    @Override
    public StellarGraphCollection union(StellarGraph graph) {
        //TODO
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Union two graph collections
     *
     * @param other other graph collection
     * @return new graph collection
     */
    @Override
    public StellarGraphCollection union(StellarGraphCollection other) {
        //TODO
        throw new UnsupportedOperationException("Not implemented yet");
    }


}
