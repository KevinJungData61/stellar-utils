package au.data61.serene.sereneutils.core.io.parquet;

import au.data61.serene.sereneutils.core.io.DataSink;
import au.data61.serene.sereneutils.core.io.common.*;
import au.data61.serene.sereneutils.core.model.epgm.GraphCollection;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

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
                .map(new GraphHeadToIO(), Encoders.bean(IOGraphHead.class))
                .write()
                .format("parquet")
                .mode("overwrite")
                .save(graphHeadPath);
        gc.getVertices()
                .map(new VertexToIO(), Encoders.bean(IOVertex.class))
                .write()
                .format("parquet")
                .mode("overwrite")
                .save(vertexPath);
        gc.getEdges()
                .map(new EdgeToIO(), Encoders.bean(IOEdge.class))
                .write()
                .format("parquet")
                .mode("overwrite")
                .save(edgePath);
    }

}
