package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.Edge;
import au.data61.serene.sereneutils.core.model.epgm.Vertex;
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
