package sh.serene.sereneutils.io.json;

import sh.serene.sereneutils.model.epgm.Edge;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from json) to Edge
 */
public class JSONToEdge extends JSONToElement implements MapFunction<Row,Edge> {

    @Override
    public Edge call(Row row) {
        String id = getId(row);
        String src = getSrc(row);
        String dst = getDst(row);
        Map<String,Object> properties = getProperties(row);
        String label = getLabel(row);
        List<String> graphs = getGraphs(row);
        return Edge.create(id, src, dst, properties, label, graphs);
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
