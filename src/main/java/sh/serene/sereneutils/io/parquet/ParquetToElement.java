package sh.serene.sereneutils.io.parquet;

import org.apache.spark.sql.Row;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;
import sh.serene.sereneutils.model.epgm.ElementId;
import sh.serene.sereneutils.model.epgm.PropertyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common methods used by ParquetToElement classes
 */
public abstract class ParquetToElement {

    /**
     * Get identifier from row
     *
     * @param row   spark dataset row
     * @return      element identifier string
     */
    protected ElementId getId(Row row) {
        return getElementId(row.getAs(ParquetConstants.IDENTIFIER));
    }

    /**
     * Get identifier from identifier struct
     *
     * @param elementIdStruct   identifier struct
     * @return                  elementId
     */
    protected ElementId getElementId(Row elementIdStruct) {
        byte[] bytes = elementIdStruct.getAs(0);
        ElementId id = new ElementId();
        id.setBytes(bytes);
        return id;
    }

    /**
     * get properties from row
     *
     * @param row   spark dataset row
     * @return      element properties
     */
    protected Map<String,PropertyValue> getProperties(Row row) {
        Map<String,PropertyValue> properties = new HashMap<>();
        scala.collection.immutable.Map<String,Row> data = row.getAs(ParquetConstants.PROPERTIES);
        Iterator<Tuple2<String,Row>> iterator = data.iterator();
        while (iterator.hasNext()) {
            Tuple2<String,Row> tuple = iterator.next();
            properties.put(tuple._1(), new PropertyValue((byte[])tuple._2().get(0)));
        }

        return properties;
    }

    /**
     * get label from row
     *
     * @param row   spark dataset row
     * @return      element label
     */
    protected String getLabel(Row row) {
        return row.getAs(ParquetConstants.LABEL);
    }

    /**
     * get graphID list from row
     *
     * @param row   spark dataset row
     * @return      list of graphs that element is contained in
     */
    protected List<ElementId> getGraphs(Row row) {
        List<ElementId> graphs = new ArrayList<>();
        List<Row> rows = row.getList(row.fieldIndex(ParquetConstants.GRAPHS));
        rows.forEach(r -> graphs.add(getElementId(r)));
        return graphs;
    }

}
