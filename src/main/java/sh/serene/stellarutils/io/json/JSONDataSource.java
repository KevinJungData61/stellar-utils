package sh.serene.stellarutils.io.json;

import sh.serene.stellarutils.io.DataSource;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.graph.spark.SparkGraphCollection;
import sh.serene.stellarutils.entities.GraphHead;
import sh.serene.stellarutils.entities.VertexCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * Data source used to read graph collections in json format
 *
 */
public class JSONDataSource implements DataSource {

    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;
    private final SparkSession spark;

    /**
     * Creates a new json data source
     *
     * @param inputPath     input epgm directory
     * @param spark         spark session
     */
    public JSONDataSource(String inputPath, SparkSession spark) {
        if (inputPath.charAt(inputPath.length() - 1) != '/') {
            inputPath += '/';
        }
        this.graphHeadPath = inputPath + JSONConstants.GRAPHS_FILE;
        this.vertexPath = inputPath + JSONConstants.VERTICES_FILE;
        this.edgePath = inputPath + JSONConstants.EDGES_FILE;
        this.spark = spark;
    }

    /**
     * Creates a new json data source
     *
     * @param graphHeadPath     input graph head path
     * @param vertexPath        input vertex path
     * @param edgePath          input edge path
     * @param spark             spark session
     */
    public JSONDataSource(String graphHeadPath, String vertexPath, String edgePath, SparkSession spark) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
        this.spark = spark;
    }

    /**
     * Read graph collection from configured paths
     *
     * @return  graph collection
     */
    @Override
    public SparkGraphCollection getGraphCollection() {
        Dataset<VertexCollection> vertexDataset = spark.read().json(this.vertexPath).map(new JSONToVertex(), Encoders.bean(VertexCollection.class));
        Dataset<EdgeCollection> edgeDataset = spark.read().json(this.edgePath).map(new JSONToEdge(), Encoders.bean(EdgeCollection.class));
        Dataset<GraphHead> graphHeadDataset = spark.read().json(this.graphHeadPath).map(new JSONToGraphHead(), Encoders.bean(GraphHead.class));
        return SparkGraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }
}
