package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.Vertex;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM vertex to a vertex that is serialisable in json format
 */
class VertexToJSON implements MapFunction<Vertex,JSONVertex> {

    @Override
    public JSONVertex call(Vertex vertex) {
        return new JSONVertex(vertex);
    }

}
