package sh.serene.sereneutils.io.parquet;

import sh.serene.sereneutils.io.DataSource;
import sh.serene.sereneutils.model.epgm.Edge;
import sh.serene.sereneutils.model.epgm.GraphCollection;
import sh.serene.sereneutils.model.epgm.GraphHead;
import sh.serene.sereneutils.model.epgm.Vertex;
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
        Dataset<GraphHead> graphHeadDataset = spark
                .read()
                .parquet(this.graphHeadPath)
                .map(new ParquetToGraphHead(), Encoders.bean(GraphHead.class));
        Dataset<Vertex> vertexDataset = spark
                .read()
                .parquet(this.vertexPath)
                .map(new ParquetToVertex(), Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark
                .read()
                .parquet(this.edgePath)
                .map(new ParquetToEdge(), Encoders.bean(Edge.class));
        return GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
    }
}
