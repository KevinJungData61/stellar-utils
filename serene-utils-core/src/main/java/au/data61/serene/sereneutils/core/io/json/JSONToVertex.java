package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.Vertex;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

public class JSONToVertex extends JSONToElement implements MapFunction<Row,Vertex> {

    @Override
    public Vertex call(Row row) {
        String id = getId(row);
        Map<String,Object> properties = getProperties(row);
        String label = getLabel(row);
        List<String> graphs = getGraphs(row);
        return Vertex.create(id, properties, label, graphs);
    }

}
