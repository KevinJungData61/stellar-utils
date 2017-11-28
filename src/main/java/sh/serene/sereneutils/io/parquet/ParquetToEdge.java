package sh.serene.sereneutils.io.parquet;

import sh.serene.sereneutils.model.epgm.EPGMEdge;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.PropertyValue;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from parquet) to EPGMEdge
 */
class ParquetToEdge extends ParquetToElement implements MapFunction<Row,EPGMEdge> {

    @Override
    public EPGMEdge call(Row row) {
        ElementId id = getId(row);
        ElementId src = getSrc(row);
        ElementId dst = getDst(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<ElementId> graphs = getGraphs(row);
        return EPGMEdge.create(id, src, dst, properties, label, graphs);
    }

    /**
     * get edge source from row
     *
     * @param row   spark dataset row
     * @return      source identifier string
     */
    private ElementId getSrc(Row row) {
        return getElementId(row.getAs(ParquetConstants.SOURCE));
    }

    /**
     * get edge target from row
     *
     * @param row   spark dataset row
     * @return      target identifier string
     */
    private ElementId getDst(Row row) {
        return getElementId(row.getAs(ParquetConstants.TARGET));
    }

}
