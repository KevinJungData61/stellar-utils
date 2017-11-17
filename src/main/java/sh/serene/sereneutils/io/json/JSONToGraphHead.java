package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.GraphHead;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import sh.serene.sereneutils.model.epgm.PropertyValue;

import java.util.Map;

/**
 * Map function from Row (as read from json) to GraphHead
 */
public class JSONToGraphHead extends JSONToElement implements MapFunction<Row,GraphHead> {

    @Override
    public GraphHead call(Row row) {
        String id = getId(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        return GraphHead.create(id, properties, label);
    }

}
