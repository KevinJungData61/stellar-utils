package sh.serene.stellarutils.model.epgm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
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
     * Spark Session
     */
    private SparkSession spark;

    /**
     * Datasets
     */
    private Dataset<GraphHead> graphHeadDataset;
    private Dataset<VertexCollection> vertexCollectionDataset;
    private Dataset<EdgeCollection> edgeCollectionDataset;

    /**
     * Lists to temporarily store elements
     */
    private List<GraphHead> graphHeads;
    private List<VertexCollection> vertexCollections;
    private List<EdgeCollection> edgeCollections;

    /**
     * Constructor
     */
    public GraphCollectionBuilder() {
        spark = SparkSession.builder().appName("Stellar Utils Graph Collection Builder").master("local").getOrCreate();
        graphHeads = new ArrayList<>();
        vertexCollections = new ArrayList<>();
        edgeCollections = new ArrayList<>();
    }

    /**
     * Creates a Graph Collection from the builder
     *
     * @return  graph collection
     */
    public GraphCollection toGraphCollection() {
        graphHeadListIntoDataset();
        vertexListIntoDataset();
        edgeListIntoDataset();
        if (graphHeadDataset == null || vertexCollectionDataset == null || edgeCollectionDataset == null) {
            return null;
        }
        return GraphCollection.fromDatasets(graphHeadDataset, vertexCollectionDataset, edgeCollectionDataset);
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
        graphHeads.add(graphHead);
        if (graphHeads.size() >= LIST_MAX_SIZE) {
            graphHeadListIntoDataset();
        }
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
        vertexCollections.add(vertex);
        if (vertexCollections.size() >= LIST_MAX_SIZE) {
            vertexListIntoDataset();
        }
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
        edgeCollections.add(edge);
        if (edgeCollections.size() >= LIST_MAX_SIZE) {
            edgeListIntoDataset();
        }
        return edge.getId();
    }

    /**
     * Move graph heads from list to dataset
     */
    private void graphHeadListIntoDataset() {
        if (this.graphHeads.isEmpty()) {
            return;
        }
        Dataset<GraphHead> graphHeads = spark.createDataset(this.graphHeads, Encoders.bean(GraphHead.class));
        graphHeadDataset = (graphHeadDataset == null) ? graphHeads : graphHeadDataset.union(graphHeads);
        this.graphHeads.clear();
    }

    /**
     * Move vertices from list to dataset
     */
    private void vertexListIntoDataset() {
        if (vertexCollections.isEmpty()) {
            return;
        }
        Dataset<VertexCollection> vertices = spark.createDataset(vertexCollections, Encoders.bean(VertexCollection.class));
        vertexCollectionDataset = (vertexCollectionDataset == null) ? vertices : vertexCollectionDataset.union(vertices);
        vertexCollections.clear();
    }

    /**
     * Move edges from list to dataset
     */
    private void edgeListIntoDataset() {
        if (edgeCollections.isEmpty()) {
            return;
        }
        Dataset<EdgeCollection> edges = spark.createDataset(edgeCollections, Encoders.bean(EdgeCollection.class));
        edgeCollectionDataset = (edgeCollectionDataset == null) ? edges : edgeCollectionDataset.union(edges);
        edgeCollections.clear();
    }

}
