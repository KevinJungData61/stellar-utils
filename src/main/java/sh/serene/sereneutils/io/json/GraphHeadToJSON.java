package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.GraphHead;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM graph head to a graph head that is serialisable in json format
 */
class GraphHeadToJSON implements MapFunction<GraphHead,JSONGraphHead> {

    @Override
    public JSONGraphHead call(GraphHead graphHead) {
        return new JSONGraphHead(graphHead);
    }

}
