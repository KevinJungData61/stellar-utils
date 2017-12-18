package sh.serene.stellarutils.io.json;

import sh.serene.stellarutils.entities.VertexCollection;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM vertex to a vertex that is serialisable in json format
 */
class VertexToJSON implements MapFunction<VertexCollection,JSONVertex> {

    @Override
    public JSONVertex call(VertexCollection vertex) {
        return new JSONVertex(vertex);
    }

}
