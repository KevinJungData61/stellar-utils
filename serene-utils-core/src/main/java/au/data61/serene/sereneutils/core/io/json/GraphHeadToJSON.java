package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.GraphHead;
import au.data61.serene.sereneutils.core.model.epgm.Vertex;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM graph head to a graph head that is serialisable in json format
 */
public class GraphHeadToJSON implements MapFunction<GraphHead,JSONGraphHead> {

    @Override
    public JSONGraphHead call(GraphHead graphHead) {
        return new JSONGraphHead(graphHead);
    }

}
