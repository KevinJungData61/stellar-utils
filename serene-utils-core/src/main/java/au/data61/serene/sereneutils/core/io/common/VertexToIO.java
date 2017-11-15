package au.data61.serene.sereneutils.core.io.common;

import au.data61.serene.sereneutils.core.io.common.IOVertex;
import au.data61.serene.sereneutils.core.model.epgm.Vertex;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps an EPGM vertex to a vertex that is serialisable in json format
 */
public class VertexToIO implements MapFunction<Vertex,IOVertex> {

    @Override
    public IOVertex call(Vertex vertex) {
        return new IOVertex(vertex);
    }

}
