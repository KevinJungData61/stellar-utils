package au.data61.serene.sereneutils.core.io.parquet;

import au.data61.serene.sereneutils.core.io.json.JSONToElement;
import au.data61.serene.sereneutils.core.model.epgm.Vertex;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from parquet) to Vertex
 */
public class ParquetToVertex extends ParquetToElement implements MapFunction<Row,Vertex> {

    @Override
    public Vertex call(Row row) {
        String id = getId(row);
        Map<String,Object> properties = getProperties(row);
        String label = getLabel(row);
        List<String> graphs = getGraphs(row);
        return Vertex.create(id, properties, label, graphs);
    }

}
