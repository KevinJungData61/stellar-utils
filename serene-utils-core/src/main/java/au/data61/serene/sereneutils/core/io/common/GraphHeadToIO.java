package au.data61.serene.sereneutils.core.io.common;

import au.data61.serene.sereneutils.core.io.common.IOGraphHead;
import au.data61.serene.sereneutils.core.model.epgm.GraphHead;
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
