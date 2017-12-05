package sh.serene.stellarutils.io.json;

import sh.serene.stellarutils.io.DataSink;
import sh.serene.stellarutils.model.epgm.GraphCollection;
import org.apache.spark.sql.Encoders;

/**
 * Data sink used to write graph collections in json format
 *
 */
public class JSONDataSink implements DataSink {

    private final String graphHeadPath;
    private final String vertexPath;
    private final String edgePath;

    /**
     * Creates a new json data sink.
     *
     * @param outputPath    output epgm directory
     */
    public JSONDataSink(String outputPath) {
        outputPath += (outputPath.charAt(outputPath.length() - 1) == '/') ? "" : "/";
        this.graphHeadPath = outputPath + JSONConstants.GRAPHS_FILE;
        this.vertexPath = outputPath + JSONConstants.VERTICES_FILE;
        this.edgePath = outputPath + JSONConstants.EDGES_FILE;
    }

    /**
     * Creates a new json data sink
     *
     * @param graphHeadPath     output graphhead path
     * @param vertexPath        output vertex path
     * @param edgePath          output edge path
     */
    public JSONDataSink(String graphHeadPath, String vertexPath, String edgePath) {
        this.graphHeadPath = graphHeadPath;
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
    }

    /**
     * Writes a graph collection in json format
     *
     * @param gc    graph collection to write
     */
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
