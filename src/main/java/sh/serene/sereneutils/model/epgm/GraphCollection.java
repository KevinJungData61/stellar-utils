package sh.serene.sereneutils.model.epgm;

import org.apache.spark.sql.Dataset;

/**
 * EPGM Graph collection representation as spark datasets
 */
public class GraphCollection {

    /**
     * EPGM Graph Heads
     */
    private final Dataset<GraphHead> graphHeads;

    /**
     * EPGM Vertices
     */
    private final Dataset<Vertex> vertices;

    /**
     * EPGM Edges
     */
    private final Dataset<Edge> edges;

    private GraphCollection(Dataset<GraphHead> graphHeads, Dataset<Vertex> vertices, Dataset<Edge> edges) {
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
    public static GraphCollection fromDatasets(Dataset<GraphHead> graphHeads, Dataset<Vertex> vertices, Dataset<Edge> edges) {
        return new GraphCollection(graphHeads, vertices, edges);
    }

    public Dataset<GraphHead> getGraphHeads() {
        return this.graphHeads;
    }

    public Dataset<Vertex> getVertices() {
        return this.vertices;
    }

    public Dataset<Edge> getEdges() {
        return this.edges;
    }

}
