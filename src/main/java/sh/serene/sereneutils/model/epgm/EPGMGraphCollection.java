package sh.serene.sereneutils.model.epgm;

import org.apache.spark.sql.Dataset;
import sh.serene.sereneutils.model.common.GraphHead;

/**
 * EPGM Graph collection representation as spark datasets
 */
public class EPGMGraphCollection {

    /**
     * EPGM Graph Heads
     */
    private final Dataset<GraphHead> graphHeads;

    /**
     * EPGM Vertices
     */
    private final Dataset<EPGMVertex> vertices;

    /**
     * EPGM Edges
     */
    private final Dataset<EPGMEdge> edges;

    private EPGMGraphCollection(Dataset<GraphHead> graphHeads, Dataset<EPGMVertex> vertices, Dataset<EPGMEdge> edges) {
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
    public static EPGMGraphCollection fromDatasets(Dataset<GraphHead> graphHeads, Dataset<EPGMVertex> vertices, Dataset<EPGMEdge> edges) {
        return new EPGMGraphCollection(graphHeads, vertices, edges);
    }

    public Dataset<GraphHead> getGraphHeads() {
        return this.graphHeads;
    }

    public Dataset<EPGMVertex> getVertices() {
        return this.vertices;
    }

    public Dataset<EPGMEdge> getEdges() {
        return this.edges;
    }

}
