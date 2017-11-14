package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.GraphHead;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.Map;

public class JSONToGraphHead extends JSONToElement implements MapFunction<Row,GraphHead> {

    @Override
    public GraphHead call(Row row) {
        String id = getId(row);
        Map<String,Object> properties = getProperties(row);
        String label = getLabel(row);
        return GraphHead.create(id, properties, label);
    }

}
