package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.EdgeCollection;
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
