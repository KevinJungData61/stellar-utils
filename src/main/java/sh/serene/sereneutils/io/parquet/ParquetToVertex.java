package sh.serene.sereneutils.io.parquet;

import sh.serene.sereneutils.model.epgm.ElementId;
import sh.serene.sereneutils.model.epgm.PropertyValue;
import sh.serene.sereneutils.model.epgm.Vertex;
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
        ElementId id = getId(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<ElementId> graphs = getGraphs(row);
        return Vertex.create(id, properties, label, graphs);
    }

}
