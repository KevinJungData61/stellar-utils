package sh.serene.stellarutils.model.epgm;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Graph Collection Builder
 *
 */
public class GraphCollectionBuilder implements Serializable {

    /**
     * List max size based on estimated memory usage
     */
    private static final int LIST_MAX_SIZE = 100000;

    /**
     * Graph elements builders
     */
    GraphElementsBuilder<GraphHead> graphHeadsBuilder;
    GraphElementsBuilder<VertexCollection> verticesBuilder;
    GraphElementsBuilder<EdgeCollection> edgesBuilder;

    /**
     * Constructor
     */
    public GraphCollectionBuilder() {
        this(
                SparkSession
                        .builder()
                        .appName("Stellar Utils Graph Collection Builder")
                        .master("local")
                        .getOrCreate()
        );
    }

    /**
     * Constructor with given spark session
     *
     * @param spark
     */
    public GraphCollectionBuilder(SparkSession spark) {
        if (spark == null) {
            throw new NullPointerException("Spark Session was null");
        }
        graphHeadsBuilder = new GraphElementsBuilder<>(GraphHead.class, LIST_MAX_SIZE, spark);
        verticesBuilder = new GraphElementsBuilder<>(VertexCollection.class, LIST_MAX_SIZE, spark);
        edgesBuilder = new GraphElementsBuilder<>(EdgeCollection.class, LIST_MAX_SIZE, spark);
    }

    /**
     * Creates a Graph Collection from the builder
     *
     * @return  graph collection
     */
    public GraphCollection toGraphCollection() {
        return GraphCollection.fromDatasets(
                graphHeadsBuilder.toElements(),
                verticesBuilder.toElements(),
                edgesBuilder.toElements()
        );
    }

    /**
     * Add a graph head
     *
     * @param properties    graph head properties
     * @param label         graph head label
     * @return              graph head ID
     */
    public ElementId addGraphHead(Map<String,PropertyValue> properties, String label) {
        GraphHead graphHead = GraphHead.create(ElementId.create(), properties, label);
        graphHeadsBuilder.add(graphHead);
        return graphHead.getId();
    }

    /**
     * Add a vertex
     *
     * @param properties    vertex properties
     * @param label         vertex label
     * @param graphs        graphs that the vertex is contained in
     * @return              vertex ID
     */
    public ElementId addVertex(Map<String,PropertyValue> properties, String label, List<ElementId> graphs) {
        VertexCollection vertex = VertexCollection.create(properties, label, graphs);
        verticesBuilder.add(vertex);
        return vertex.getId();
    }

    /**
     * Add an edge
     *
     * @param src           edge source
     * @param dst           edge destination
     * @param properties    edge properties
     * @param label         edge label
     * @param graphs        graphs that the edge is contained in
     * @return              edge ID
     */
    public ElementId addEdge(
            ElementId src,
            ElementId dst,
            Map<String,PropertyValue> properties,
            String label,
            List<ElementId> graphs
    ) {
        EdgeCollection edge = EdgeCollection.create(src, dst, properties, label, graphs);
        edgesBuilder.add(edge);
        return edge.getId();
    }

}
