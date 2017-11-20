package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.Edge;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM edge to a edge that is serialisable in json format
 */
public class EdgeToJSON implements MapFunction<Edge,JSONEdge> {

    @Override
    public JSONEdge call(Edge edge) {
        return new JSONEdge(edge);
    }

}
