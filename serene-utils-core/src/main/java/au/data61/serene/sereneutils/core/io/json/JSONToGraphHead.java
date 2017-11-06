package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.GraphHead;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

public class JSONToGraphHead extends JSONToElement implements Function<String,GraphHead> {

    @Override
    public GraphHead call(String s) throws ParseException {
        JSONObject jsonGraphHead = (JSONObject) (new JSONParser()).parse(s);
        String id = getId(jsonGraphHead);
        Map<String,Object> properties = getProperties(jsonGraphHead);
        String label = getLabel(jsonGraphHead);
        return GraphHead.create(id, properties, label);
    }

}
