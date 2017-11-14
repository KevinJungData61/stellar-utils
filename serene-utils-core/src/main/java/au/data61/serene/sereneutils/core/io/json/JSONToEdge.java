package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.Edge;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

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

    private String getSrc(Row row) {
        return row.getAs("source");
    }

    private String getDst(Row row) {
        return row.getAs("target");
    }

}
