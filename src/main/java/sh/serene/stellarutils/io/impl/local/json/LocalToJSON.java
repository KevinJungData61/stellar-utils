package sh.serene.stellarutils.io.impl.local.json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.io.impl.spark.json.JSONConstants;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class LocalToJSON {

    protected JSONObject getJSONProperties(Map<String,PropertyValue> properties) {
        JSONObject props = new JSONObject();
        for (Map.Entry<String,PropertyValue> entry : properties.entrySet()) {
            props.put(entry.getKey(), entry.getValue().value());
        }
        return props;
    }

    protected JSONArray getJSONGraphIds(List<ElementId> graphs) {
        JSONArray jsonGraphs = new JSONArray();
        for (ElementId graphId : graphs) {
            jsonGraphs.add(graphId.toString());
        }
        return jsonGraphs;
    }

    private static class GraphHeadToJSON extends LocalToJSON implements Function<GraphHead,String> {

        /**
         * Applies this function to the given argument.
         *
         * @param graphHead the function argument
         * @return the function result
         */
        @Override
        public String apply(GraphHead graphHead) {
            JSONObject json = new JSONObject();
            JSONObject props = getJSONProperties(graphHead.getProperties());
            JSONObject meta = new JSONObject();
            meta.put(JSONConstants.LABEL, graphHead.getLabel());
            json.put(JSONConstants.IDENTIFIER, graphHead.getId().toString());
            json.put(JSONConstants.PROPERTIES, props);
            json.put(JSONConstants.META, meta);
            return json.toString();
        }
    }

    private static class VertexCollectionToJSON extends LocalToJSON implements Function<VertexCollection,String> {

        /**
         * Applies this function to the given argument.
         *
         * @param vertexCollection the function argument
         * @return the function result
         */
        @Override
        public String apply(VertexCollection vertexCollection) {
            JSONObject json = new JSONObject();
            JSONObject props = getJSONProperties(vertexCollection.getProperties());
            JSONArray graphs = getJSONGraphIds(vertexCollection.getGraphs());
            JSONObject meta = new JSONObject();
            meta.put(JSONConstants.LABEL, vertexCollection.getLabel());
            meta.put(JSONConstants.GRAPHS, graphs);
            json.put(JSONConstants.IDENTIFIER, vertexCollection.getId().toString());
            json.put(JSONConstants.PROPERTIES, props);
            json.put(JSONConstants.META, meta);
            return json.toString();
        }
    }

    private static class EdgeCollectionToJSON extends LocalToJSON implements Function<EdgeCollection,String> {

        /**
         * Applies this function to the given argument.
         *
         * @param edgeCollection the function argument
         * @return the function result
         */
        @Override
        public String apply(EdgeCollection edgeCollection) {
            JSONObject json = new JSONObject();
            JSONObject props = getJSONProperties(edgeCollection.getProperties());
            JSONArray graphs = getJSONGraphIds(edgeCollection.getGraphs());
            JSONObject meta = new JSONObject();
            meta.put(JSONConstants.LABEL, edgeCollection.getLabel());
            meta.put(JSONConstants.GRAPHS, graphs);
            json.put(JSONConstants.IDENTIFIER, edgeCollection.getId().toString());
            json.put(JSONConstants.SOURCE, edgeCollection.getSrc().toString());
            json.put(JSONConstants.TARGET, edgeCollection.getDst().toString());
            json.put(JSONConstants.PROPERTIES, props);
            json.put(JSONConstants.META, meta);
            return json.toString();
        }
    }

    public static GraphHeadToJSON fromGraphHead() {
        return new GraphHeadToJSON();
    }

    public static VertexCollectionToJSON fromVertexCollection() {
        return new VertexCollectionToJSON();
    }

    public static EdgeCollectionToJSON fromEdgeCollection() {
        return new EdgeCollectionToJSON();
    }
}
