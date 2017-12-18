package sh.serene.stellarutils.io.json;

import sh.serene.stellarutils.entities.GraphHead;
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
