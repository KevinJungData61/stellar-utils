package sh.serene.sereneutils.io.common;

import sh.serene.sereneutils.model.epgm.GraphHead;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM graph head to a graph head that is serialisable in json format
 */
public class GraphHeadToIO implements MapFunction<GraphHead,IOGraphHead> {

    @Override
    public IOGraphHead call(GraphHead graphHead) {
        return new IOGraphHead(graphHead);
    }

}
