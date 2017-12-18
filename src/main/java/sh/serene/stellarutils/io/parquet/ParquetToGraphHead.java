package sh.serene.stellarutils.io.parquet;

import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.GraphHead;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import sh.serene.stellarutils.entities.PropertyValue;

import java.util.Map;

/**
 * Map function from Row (as read from parquet) to GraphHead
 */
class ParquetToGraphHead extends ParquetToElement implements MapFunction<Row,GraphHead> {

    @Override
    public GraphHead call(Row row) {
        ElementId id = getId(row);
        Map<String,PropertyValue> properties = getProperties(row);
        String label = getLabel(row);
        return GraphHead.create(id, properties, label);
    }

}
