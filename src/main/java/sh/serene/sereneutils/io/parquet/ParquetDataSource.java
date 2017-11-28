package sh.serene.sereneutils.io.parquet;

import sh.serene.sereneutils.io.DataSource;
import sh.serene.sereneutils.model.epgm.EPGMEdge;
import sh.serene.sereneutils.model.epgm.EPGMGraphCollection;
import sh.serene.sereneutils.model.common.GraphHead;
import sh.serene.sereneutils.model.epgm.EPGMVertex;
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
        this(inputPath + ParquetConstants.GRAPHS_FILE,
                inputPath + ParquetConstants.VERTICES_FILE,
                inputPath + ParquetConstants.EDGES_FILE,
                spark);
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
    public EPGMGraphCollection getGraphCollection() {
        Dataset<GraphHead> graphHeadDataset = spark
                .read()
                .parquet(this.graphHeadPath)
                .map(new ParquetToGraphHead(), Encoders.bean(GraphHead.class));
        Dataset<EPGMVertex> vertexDataset = spark
                .read()
                .parquet(this.vertexPath)
                .map(new ParquetToVertex(), Encoders.bean(EPGMVertex.class));
        Dataset<EPGMEdge> edgeDataset = spark
                .read()
                .parquet(this.edgePath)
                .map(new ParquetToEdge(), Encoders.bean(EPGMEdge.class));
        return EPGMGraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }
}
