package au.data61.serene.sereneutils.core.model;

import org.apache.spark.sql.Dataset;

public class GraphCollection {

    private final Dataset<GraphHead> graphHeads;
    private final Dataset<Vertex> vertices;
    private final Dataset<Edge> edges;

    private GraphCollection(Dataset<GraphHead> graphHeads, Dataset<Vertex> vertices, Dataset<Edge> edges) {
        this.graphHeads = graphHeads;
        this.vertices = vertices;
        this.edges = edges;
    }

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
