package sh.serene.stellarutils.io.parquet;

import sh.serene.stellarutils.io.DataSource;
import sh.serene.stellarutils.model.epgm.EdgeCollection;
import sh.serene.stellarutils.model.epgm.GraphCollection;
import sh.serene.stellarutils.model.epgm.GraphHead;
import sh.serene.stellarutils.model.epgm.VertexCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * Parquet data source
 */
public class ParquetDataSource implements DataSource {

    /**
     * input paths
     */
    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;
    private final SparkSession spark;

    /**
     * Create a new parquet data source
     *
     * @param inputPath     input directory holding default directories for graph heads, vertices, and edges
     * @param spark         spark session to create datasets with
     */
    public ParquetDataSource(String inputPath, SparkSession spark) {
        if (inputPath.charAt(inputPath.length() - 1) != '/') {
            inputPath += '/';
        }
        this.graphHeadPath = inputPath + ParquetConstants.GRAPHS_FILE;
        this.vertexPath = inputPath + ParquetConstants.VERTICES_FILE;
        this.edgePath = inputPath + ParquetConstants.EDGES_FILE;
        this.spark = spark;
    }

    /**
     * Create a new parquet data source
     *
     * @param graphHeadPath     input directory for graph heads
     * @param vertexPath        input directory for vertices
     * @param edgePath          input directory for edges
     * @param spark             spark session to create datasets with
     */
    public ParquetDataSource(String graphHeadPath, String vertexPath, String edgePath, SparkSession spark) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
        this.spark = spark;
    }

    /**
     * Create a graph collection from the parquet data source
     *
     * @return  Graph collection
     */
    @Override
    public GraphCollection getGraphCollection() {
        Dataset<GraphHead> graphHeadDataset = spark
                .read()
                .parquet(this.graphHeadPath)
                .map(new ParquetToGraphHead(), Encoders.bean(GraphHead.class));
        Dataset<VertexCollection> vertexDataset = spark
                .read()
                .parquet(this.vertexPath)
                .map(new ParquetToVertex(), Encoders.bean(VertexCollection.class));
        Dataset<EdgeCollection> edgeDataset = spark
                .read()
                .parquet(this.edgePath)
                .map(new ParquetToEdge(), Encoders.bean(EdgeCollection.class));
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }
}
