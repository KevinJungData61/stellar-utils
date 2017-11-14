package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.Vertex;
import org.apache.spark.api.java.function.MapFunction;

public class VertexToJSON implements MapFunction<Vertex,JSONVertex> {

    @Override
    public JSONVertex call(Vertex vertex) {
        return new JSONVertex(vertex);
    }

}
