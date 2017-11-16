package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.io.DataSink;
import sh.serene.sereneutils.model.epgm.GraphCollection;
import org.apache.spark.sql.Encoders;
import sh.serene.sereneutils.io.common.*;

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
        this(outputPath + JSONConstants.GRAPHS_FILE,
                outputPath + JSONConstants.VERTICES_FILE,
                outputPath + JSONConstants.EDGES_FILE);
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
                .map(new GraphHeadToIO(), Encoders.bean(IOGraphHead.class))
                .write()
                .mode("overwrite")
                .json(this.graphHeadPath);
        gc.getVertices()
                .map(new VertexToIO(), Encoders.bean(IOVertex.class))
                .write()
                .mode("overwrite")
                .json(this.vertexPath);
        gc.getEdges()
                .map(new EdgeToIO(), Encoders.bean(IOEdge.class))
                .write()
                .mode("overwrite")
                .json(this.edgePath);
    }
}