package sh.serene.stellarutils.io.json;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import sh.serene.stellarutils.model.epgm.PropertyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common methods used by JSONToElement classes
 */
abstract class JSONToElement {

    /**
     * Get identifier string from row
     *
     * @param row   spark dataset row
     * @return      element identifier string
     */
    protected String getId(Row row) {
        return row.getAs(JSONConstants.IDENTIFIER);
    }

    /**
     * get properties from row
     *
     * @param row   spark dataset row
     * @return      element properties
     */
    protected Map<String,PropertyValue> getProperties(Row row) {
        Map<String,PropertyValue> properties = new HashMap<>();
        try {
            Row data = row.getAs(JSONConstants.PROPERTIES);
            for (StructField sf : data.schema().fields()) {
                String value = data.getAs(sf.name());
                if (value != null) {
                    properties.put(sf.name(), PropertyValue.create(value));
                }
            }
            return properties;
        } catch (IllegalArgumentException e) {
            return properties;
        }
    }

    /**
     * get label from row
     *
     * @param row   spark dataset row
     * @return      element label
     */
    protected String getLabel(Row row) {
        return ((Row) row.getAs(JSONConstants.META)).getAs(JSONConstants.LABEL);
    }

    /**
     * get graphID list from row
     *
     * @param row   spark dataset row
     * @return      list of graphs that element is contained in
     */
    protected List<String> getGraphs(Row row) {
        Row meta = row.getAs(JSONConstants.META);
        return meta.getList(meta.fieldIndex(JSONConstants.GRAPHS));
    }

}
