package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.Vertex;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;
import java.util.Map;

public class JSONToVertex extends JSONToElement implements Function<String,Vertex> {

    @Override
    public Vertex call(String s) throws ParseException {
        JSONObject jsonVertex = (JSONObject) (new JSONParser()).parse(s);
        String id = getId(jsonVertex);
        Map<String,String> properties = getProperties(jsonVertex);
        String label = getLabel(jsonVertex);
        List<String> graphs = getGraphs(jsonVertex);
        return Vertex.create(id, properties, label, graphs);

    }

}
