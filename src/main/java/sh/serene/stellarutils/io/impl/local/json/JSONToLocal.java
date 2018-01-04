package sh.serene.stellarutils.io.impl.local.json;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.io.impl.spark.json.JSONConstants;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;

public class JSONToLocal {

    protected JSONParser parser;

    private JSONToLocal() {
        parser = new JSONParser();
    }

    protected ElementId getId(JSONObject json) {
        return ElementId.fromString((String) json.get(JSONConstants.IDENTIFIER));
    }

    protected String getLabel(JSONObject json) {
        return (String) (((JSONObject) json.get(JSONConstants.META)).get(JSONConstants.LABEL));
    }

    protected Map<String,PropertyValue> getProperties(JSONObject json) {
        Map<String,PropertyValue> properties = new HashMap<>();
        JSONObject jsonProps = (JSONObject) json.get(JSONConstants.PROPERTIES);
        Set<Map.Entry> entrySet = jsonProps.entrySet();
        for (Map.Entry entry : entrySet) {
            properties.put((String) entry.getKey(), PropertyValue.create(entry.getValue()));
        }
        return properties;
    }

    protected List<ElementId> getGraphs(JSONObject json) {
        List<ElementId> graphs = new ArrayList<>();
        JSONArray jsonGraphs = (JSONArray) (((JSONObject) json.get(JSONConstants.META)).get(JSONConstants.GRAPHS));
        jsonGraphs.forEach((id) -> {
            graphs.add(ElementId.fromString((String) id));
        });
        return graphs;
    }

    private static class JSONToVertexCollection extends JSONToLocal implements Function<String,VertexCollection> {

        @Override
        public VertexCollection apply(String s) {
            try {
                JSONObject json = (JSONObject) parser.parse(s);
                ElementId id = getId(json);
                String label = getLabel(json);
                Map<String,PropertyValue> properties = getProperties(json);
                List<ElementId> graphs = getGraphs(json);
                return VertexCollection.create(id, properties, label, graphs);
            } catch (ParseException e) {
                throw new UncheckedIOException(new IOException(e.toString()));
            }
        }


    }

    private static class JSONToEdgeCollection extends JSONToLocal implements Function<String,EdgeCollection> {
        @Override
        public EdgeCollection apply(String s) {
            try {
                JSONObject json = (JSONObject) parser.parse(s);
                ElementId id = getId(json);
                ElementId src = getSrc(json);
                ElementId dst = getDst(json);
                String label = getLabel(json);
                Map<String,PropertyValue> properties = getProperties(json);
                List<ElementId> graphs = getGraphs(json);
                return EdgeCollection.create(id, src, dst, properties, label, graphs);
            } catch (ParseException e) {
                throw new UncheckedIOException(new IOException(e.toString()));
            }
        }

        private ElementId getSrc(JSONObject json) {
            return ElementId.fromString((String) json.get(JSONConstants.SOURCE));
        }

        private ElementId getDst(JSONObject json) {
            return ElementId.fromString((String) json.get(JSONConstants.TARGET));
        }
    }

    private static class JSONToGraphHead extends JSONToLocal implements Function<String,GraphHead> {
        @Override
        public GraphHead apply(String s) {
            try {
                JSONObject json = (JSONObject) parser.parse(s);
                ElementId id = getId(json);
                String label = getLabel(json);
                Map<String,PropertyValue> properties = getProperties(json);
                return GraphHead.create(id, properties, label);
            } catch (ParseException e) {
                throw new UncheckedIOException(new IOException(e.toString()));
            }
        }
    }

    public static JSONToVertexCollection toVertexCollection() {
        return new JSONToVertexCollection();
    }

    public static JSONToEdgeCollection toEdgeCollection() {
        return new JSONToEdgeCollection();
    }

    public static JSONToGraphHead toGraphHead() {
        return new JSONToGraphHead();
    }

}
