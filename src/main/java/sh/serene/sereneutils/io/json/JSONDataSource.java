package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.io.DataSource;
import sh.serene.sereneutils.model.epgm.EPGMEdge;
import sh.serene.sereneutils.model.common.GraphHead;
import sh.serene.sereneutils.model.epgm.EPGMVertex;
import sh.serene.sereneutils.model.epgm.EPGMGraphCollection;
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
        this(inputPath + JSONConstants.GRAPHS_FILE,
                inputPath + JSONConstants.VERTICES_FILE,
                inputPath + JSONConstants.EDGES_FILE,
                spark);
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
    public EPGMGraphCollection getGraphCollection() {
        Dataset<EPGMVertex> vertexDataset = spark.read().json(this.vertexPath).map(new JSONToVertex(), Encoders.bean(EPGMVertex.class));
        Dataset<EPGMEdge> edgeDataset = spark.read().json(this.edgePath).map(new JSONToEdge(), Encoders.bean(EPGMEdge.class));
        Dataset<GraphHead> graphHeadDataset = spark.read().json(this.graphHeadPath).map(new JSONToGraphHead(), Encoders.bean(GraphHead.class));
        return EPGMGraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }
}
