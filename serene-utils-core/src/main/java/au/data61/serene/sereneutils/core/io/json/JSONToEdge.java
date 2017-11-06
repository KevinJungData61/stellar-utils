package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.Edge;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;
import java.util.Map;

public class JSONToEdge extends JSONToElement implements Function<String,Edge> {

    @Override
    public Edge call(String s) throws ParseException {
        JSONObject jsonEdge = (JSONObject) (new JSONParser()).parse(s);
        String id = getId(jsonEdge);
        String src = getSrc(jsonEdge);
        String dst = getDst(jsonEdge);
        Map<String,Object> properties = getProperties(jsonEdge);
        String label = getLabel(jsonEdge);
        List<String> graphs = getGraphs(jsonEdge);
        return Edge.create(id, src, dst, properties, label, graphs);
    }

    private String getSrc(JSONObject obj) {
        return (String) obj.get(JSONConstants.SOURCE);
    }

    private String getDst(JSONObject obj) {
        return (String) obj.get(JSONConstants.TARGET);
    }

}
