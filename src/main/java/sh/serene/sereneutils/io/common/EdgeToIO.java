package sh.serene.sereneutils.io.common;

import sh.serene.sereneutils.model.epgm.Edge;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM edge to a edge that is serialisable in json format
 */
public class EdgeToIO implements MapFunction<Edge,IOEdge> {

    @Override
    public IOEdge call(Edge edge) {
        return new IOEdge(edge);
    }

}
