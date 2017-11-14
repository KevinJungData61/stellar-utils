package au.data61.serene.sereneutils.core.io.json;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class JSONToElement {

    protected String getId(Row row) {
        return row.getAs("id");
    }

    protected Map<String,Object> getProperties(Row row) {
        Map<String,Object> properties = new HashMap<>();
        try {
            Row data = row.getAs("data");
            for (StructField sf : data.schema().fields()) {
                Object value = data.getAs(sf.name());
                if (value != null) {
                    properties.put(sf.name(), value);
                }
            }
        } catch (IllegalArgumentException e) {
        } finally {
            return properties;
        }
    }

    protected String getLabel(Row row) {
        return ((Row) row.getAs("meta")).getAs("label");
    }

    protected List<String> getGraphs(Row row) {
        Row meta = row.getAs("meta");
        return meta.getList(meta.fieldIndex("graphs"));
    }

}
