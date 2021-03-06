package sh.serene.stellarutils.io.json;

import sh.serene.stellarutils.model.epgm.EdgeCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import sh.serene.stellarutils.model.epgm.PropertyValue;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from json) to EdgeCollection
 */
class JSONToEdge extends JSONToElement implements MapFunction<Row,EdgeCollection> {

    @Override
    public EdgeCollection call(Row row) {
        String id = getId(row);
        String src = getSrc(row);
        String dst = getDst(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<String> graphs = getGraphs(row);
        return EdgeCollection.createFromStringIds(id, src, dst, properties, label, graphs);
    }

    /**
     * get edge source from row
     *
     * @param row   spark dataset row
     * @return      source identifier string
     */
    private String getSrc(Row row) {
        return row.getAs(JSONConstants.SOURCE);
    }

    /**
     * get edge target from row
     *
     * @param row   spark dataset row
     * @return      target identifier string
     */
    private String getDst(Row row) {
        return row.getAs(JSONConstants.TARGET);
    }

}
