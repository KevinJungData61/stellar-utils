package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.Element;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class JSONToElement {

    protected String getId(JSONObject obj) {
        return (String) obj.get(JSONConstants.IDENTIFIER);
    }

    protected Map<String,Object> getProperties(JSONObject obj) {
        Map<String,Object> properties = new HashMap<>();
        JSONObject jsonProperties = (JSONObject) obj.get(JSONConstants.PROPERTIES);
        jsonProperties.keySet().forEach(key -> {
            String keyStr = key.toString();
            properties.put(keyStr, properties.get(keyStr));
        });
        return properties;
    }

    protected String getLabel(JSONObject obj) {
        return (String) ((JSONObject) obj.get(JSONConstants.META)).get(JSONConstants.LABEL);
    }

    protected List<String> getGraphs(JSONObject obj) {
        return (List<String>) ((JSONObject) obj.get(JSONConstants.META)).get(JSONConstants.GRAPHS);
    }

}
