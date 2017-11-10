package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.io.DataSource;
import au.data61.serene.sereneutils.core.model.Edge;
import au.data61.serene.sereneutils.core.model.GraphCollection;
import au.data61.serene.sereneutils.core.model.GraphHead;
import au.data61.serene.sereneutils.core.model.Vertex;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class JSONDataSource implements DataSource {

    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;
    private final SparkSession spark;

    public JSONDataSource(String inputPath, SparkSession spark) {
        this(inputPath + JSONConstants.GRAPHS_FILE,
                inputPath + JSONConstants.VERTICES_FILE,
                inputPath + JSONConstants.EDGES_FILE,
                spark);
    }

    public JSONDataSource(String graphHeadPath, String vertexPath, String edgePath, SparkSession spark) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
        this.spark = spark;
    }

    @Override
    public GraphCollection getGraphCollection() {
        JavaRDD<Vertex> vertexRDD = spark.sparkContext()
                .textFile(vertexPath, 1)
                .toJavaRDD()
                .map(new JSONToVertex());
        Dataset<Vertex> vertexDataset = spark.createDataset(vertexRDD.rdd(), Encoders.bean(Vertex.class));

        JavaRDD<Edge> edgeRDD = spark.sparkContext()
                .textFile(edgePath, 1)
                .toJavaRDD()
                .map(new JSONToEdge());
        Dataset<Edge> edgeDataset = spark.createDataset(edgeRDD.rdd(), Encoders.bean(Edge.class));

        JavaRDD<GraphHead> graphHeadRDD = spark.sparkContext()
                .textFile(graphHeadPath, 1)
                .toJavaRDD()
                .map(new JSONToGraphHead());
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphHeadRDD.rdd(), Encoders.bean(GraphHead.class));

        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);

    }
}
