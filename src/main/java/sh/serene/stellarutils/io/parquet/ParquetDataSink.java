package sh.serene.stellarutils.io.parquet;

import sh.serene.stellarutils.io.DataSink;
import sh.serene.stellarutils.model.epgm.GraphCollection;

/**
 * Parquet data sink
 */
public class ParquetDataSink implements DataSink {

    /**
     * Paths to write parquet files
     */
    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;

    /**
     * Create a new parquet data sink
     *
     * @param outputPath    output directory
     */
    public ParquetDataSink(String outputPath) {
        outputPath += (outputPath.charAt(outputPath.length() - 1) == '/') ? "" : "/";
        this.graphHeadPath = outputPath + ParquetConstants.GRAPHS_FILE;
        this.vertexPath = outputPath + ParquetConstants.VERTICES_FILE;
        this.edgePath = outputPath + ParquetConstants.EDGES_FILE;
    }

    /**
     * Create a new parquet data sink
     *
     * @param graphHeadPath     output directory for graph heads
     * @param vertexPath        output directory for vertices
     * @param edgePath          output directory for edges
     */
    public ParquetDataSink(String graphHeadPath, String vertexPath, String edgePath) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
    }

    /**
     * Write a graph collection in parquet format
     *
     * @param gc    Graph collection
     */
    public void writeGraphCollection(GraphCollection gc) {
        gc.getGraphHeads()
                .write()
                .format("parquet")
                .mode("overwrite")
                .save(graphHeadPath);
        gc.getVertices()
                .write()
                .format("parquet")
                .mode("overwrite")
                .save(vertexPath);
        gc.getEdges()
                .write()
                .format("parquet")
                .mode("overwrite")
                .save(edgePath);
    }

}
