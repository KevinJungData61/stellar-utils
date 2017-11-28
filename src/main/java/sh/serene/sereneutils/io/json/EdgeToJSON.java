package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.EPGMEdge;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM edge to a edge that is serialisable in json format
 */
class EdgeToJSON implements MapFunction<EPGMEdge,JSONEdge> {

    @Override
    public JSONEdge call(EPGMEdge edge) {
        return new JSONEdge(edge);
    }

}
