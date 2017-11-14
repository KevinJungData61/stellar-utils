package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.GraphCollection;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class JSONDataSink {

    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;
    private final SparkSession spark;

    public JSONDataSink(String outputPath, SparkSession spark) {
        this(outputPath + JSONConstants.GRAPHS_FILE,
                outputPath + JSONConstants.VERTICES_FILE,
                outputPath + JSONConstants.EDGES_FILE,
                spark);
    }

    public JSONDataSink(String graphHeadPath, String vertexPath, String edgePath, SparkSession spark) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
        this.spark = spark;
    }

    public void writeGraphCollection(GraphCollection gc) {
        gc.getGraphHeads()
                .map(new GraphHeadToJSON(), Encoders.bean(JSONGraphHead.class))
                .write()
                .mode("overwrite")
                .json(this.graphHeadPath);
        gc.getVertices()
                .map(new VertexToJSON(), Encoders.bean(JSONVertex.class))
                .write()
                .mode("overwrite")
                .json(this.vertexPath);
        gc.getEdges()
                .map(new EdgeToJSON(), Encoders.bean(JSONEdge.class))
                .write()
                .mode("overwrite")
                .json(this.edgePath);
    }
}
