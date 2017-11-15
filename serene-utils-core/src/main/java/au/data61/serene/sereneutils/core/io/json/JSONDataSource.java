package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.io.DataSource;
import au.data61.serene.sereneutils.core.model.epgm.Edge;
import au.data61.serene.sereneutils.core.model.epgm.GraphCollection;
import au.data61.serene.sereneutils.core.model.epgm.GraphHead;
import au.data61.serene.sereneutils.core.model.epgm.Vertex;
import org.apache.spark.api.java.JavaRDD;
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
    public GraphCollection getGraphCollection() {
        Dataset<Vertex> vertexDataset = spark.read().json(this.vertexPath).map(new JSONToVertex(), Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark.read().json(this.edgePath).map(new JSONToEdge(), Encoders.bean(Edge.class));
        Dataset<GraphHead> graphHeadDataset = spark.read().json(this.graphHeadPath).map(new JSONToGraphHead(), Encoders.bean(GraphHead.class));
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }
}
