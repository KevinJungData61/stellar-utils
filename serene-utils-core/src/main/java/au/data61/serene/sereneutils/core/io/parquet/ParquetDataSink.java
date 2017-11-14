package au.data61.serene.sereneutils.core.io.parquet;

import au.data61.serene.sereneutils.core.model.epgm.GraphCollection;
import org.apache.spark.sql.SparkSession;

public class ParquetDataSink {

    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;
    private final SparkSession spark;

    public ParquetDataSink(String outputPath, SparkSession spark) {
        this(outputPath + ParquetConstants.GRAPHS_FILE,
                outputPath + ParquetConstants.VERTICES_FILE,
                outputPath + ParquetConstants.EDGES_FILE,
                spark);
    }

    public ParquetDataSink(String graphHeadPath, String vertexPath, String edgePath, SparkSession spark) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
        this.spark = spark;
    }

    public void writeGraphCollection(GraphCollection gc) {
        gc.getGraphHeads().write().format("parquet").mode("overwrite").save(graphHeadPath);
        gc.getVertices().write().format("parquet").mode("overwrite").save(vertexPath);
        gc.getEdges().write().format("parquet").mode("overwrite").save(edgePath);
    }

}
