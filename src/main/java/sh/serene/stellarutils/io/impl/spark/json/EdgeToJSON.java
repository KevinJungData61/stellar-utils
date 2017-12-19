package sh.serene.stellarutils.io.impl.spark.json;

import sh.serene.stellarutils.entities.EdgeCollection;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM edge to a edge that is serialisable in json format
 */
class EdgeToJSON implements MapFunction<EdgeCollection,JSONEdge> {

    @Override
    public JSONEdge call(EdgeCollection edge) {
        return new JSONEdge(edge);
    }

}
