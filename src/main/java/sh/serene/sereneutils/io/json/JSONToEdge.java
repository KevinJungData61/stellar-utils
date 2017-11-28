package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.EPGMEdge;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import sh.serene.sereneutils.model.common.PropertyValue;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from json) to EPGMEdge
 */
class JSONToEdge extends JSONToElement implements MapFunction<Row,EPGMEdge> {

    @Override
    public EPGMEdge call(Row row) {
        String id = getId(row);
        String src = getSrc(row);
        String dst = getDst(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<String> graphs = getGraphs(row);
        return EPGMEdge.createFromStringIds(id, src, dst, properties, label, graphs);
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
