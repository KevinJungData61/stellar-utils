package sh.serene.sereneutils.io.parquet;

import sh.serene.sereneutils.model.epgm.Edge;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import sh.serene.sereneutils.model.epgm.ElementId;
import sh.serene.sereneutils.model.epgm.PropertyValue;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from parquet) to Edge
 */
public class ParquetToEdge extends ParquetToElement implements MapFunction<Row,Edge> {

    @Override
    public Edge call(Row row) {
        ElementId id = getId(row);
        ElementId src = getSrc(row);
        ElementId dst = getDst(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<ElementId> graphs = getGraphs(row);
        return Edge.create(id, src, dst, properties, label, graphs);
    }

    /**
     * get edge source from row
     *
     * @param row   spark dataset row
     * @return      source identifier string
     */
    private ElementId getSrc(Row row) {
        return row.getAs(ParquetConstants.SOURCE);
    }

    /**
     * get edge target from row
     *
     * @param row   spark dataset row
     * @return      target identifier string
     */
    private ElementId getDst(Row row) {
        return row.getAs(ParquetConstants.TARGET);
    }

}
