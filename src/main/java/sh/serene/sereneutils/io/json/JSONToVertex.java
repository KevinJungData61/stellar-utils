package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.EPGMVertex;
import sh.serene.sereneutils.model.common.PropertyValue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from json) to EPGMVertex
 */
class JSONToVertex extends JSONToElement implements MapFunction<Row,EPGMVertex> {

    @Override
    public EPGMVertex call(Row row) {
        String id = getId(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<String> graphs = getGraphs(row);
        return EPGMVertex.createFromStringIds(id, properties, label, graphs);
    }

}
