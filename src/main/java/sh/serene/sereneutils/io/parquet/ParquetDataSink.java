package sh.serene.sereneutils.io.parquet;

import sh.serene.sereneutils.io.DataSink;
import sh.serene.sereneutils.model.epgm.GraphCollection;

public class ParquetDataSink implements DataSink {

    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;

    public ParquetDataSink(String outputPath) {
        this(outputPath + ParquetConstants.GRAPHS_FILE,
                outputPath + ParquetConstants.VERTICES_FILE,
                outputPath + ParquetConstants.EDGES_FILE);
    }

    public ParquetDataSink(String graphHeadPath, String vertexPath, String edgePath) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
    }

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
