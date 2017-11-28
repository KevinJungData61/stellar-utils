package sh.serene.sereneutils.io.parquet;

import sh.serene.sereneutils.model.epgm.EPGMVertex;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.PropertyValue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * Map function from Row (as read from parquet) to EPGMVertex
 */
class ParquetToVertex extends ParquetToElement implements MapFunction<Row,EPGMVertex> {

    @Override
    public EPGMVertex call(Row row) {
        ElementId id = getId(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        List<ElementId> graphs = getGraphs(row);
        return EPGMVertex.create(id, properties, label, graphs);
    }

}
