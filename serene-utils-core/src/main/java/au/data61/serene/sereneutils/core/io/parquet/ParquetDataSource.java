package au.data61.serene.sereneutils.core.io.parquet;

import au.data61.serene.sereneutils.core.io.DataSource;
import au.data61.serene.sereneutils.core.model.Edge;
import au.data61.serene.sereneutils.core.model.GraphCollection;
import au.data61.serene.sereneutils.core.model.GraphHead;
import au.data61.serene.sereneutils.core.model.Vertex;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ParquetDataSource implements DataSource {

    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;
    private final SparkSession spark;

    public ParquetDataSource(String inputPath, SparkSession spark) {
        this(inputPath + ParquetConstants.GRAPHS_FILE,
                inputPath + ParquetConstants.VERTICES_FILE,
                inputPath + ParquetConstants.EDGES_FILE,
                spark);
    }

    public ParquetDataSource(String graphHeadPath, String vertexPath, String edgePath, SparkSession spark) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
        this.spark = spark;
    }

    @Override
    public GraphCollection getGraphCollection() {
        Dataset<GraphHead> graphHeadDataset = spark.read().parquet(this.graphHeadPath).as(Encoders.bean(GraphHead.class));
        Dataset<Vertex> vertexDataset = spark.read().parquet(this.vertexPath).as(Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark.read().parquet(this.edgePath).as(Encoders.bean(Edge.class));
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }
}
