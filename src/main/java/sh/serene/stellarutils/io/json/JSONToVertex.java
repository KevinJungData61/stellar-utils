package sh.serene.stellarutils.io.json;

import sh.serene.stellarutils.model.epgm.VertexCollection;
import sh.serene.stellarutils.model.epgm.PropertyValue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from json) to VertexCollection
 */
class JSONToVertex extends JSONToElement implements MapFunction<Row,VertexCollection> {

    @Override
    public VertexCollection call(Row row) {
        String id = getId(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<String> graphs = getGraphs(row);
        return VertexCollection.createFromStringIds(id, properties, label, graphs);
    }

}
