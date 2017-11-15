package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.ElementId;
import au.data61.serene.sereneutils.core.model.epgm.Vertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * EPGM vertex with fields that can be serialised in json format
 */
public class JSONVertex implements Serializable {

    private String id;
    private Map<String,String> data;
    private Meta meta;

    public static class Meta {
        private String label;
        private List<String> graphs;

        public Meta() { }

        Meta(String label, List<ElementId> graphs) {
            this.label = label;
            this.graphs = new ArrayList<>();
            graphs.forEach(elementId -> this.graphs.add(elementId.toString()));
        }

        public String getLabel() {
            return this.label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public List<String> getGraphs() {
            return this.graphs;
        }

        public void setGraphs(List<String> graphs) {
            this.graphs = graphs;
        }
    }

    public JSONVertex() { }

    /**
     * Creates a new json serialisable vertex given an EPGM vertex
     *
     * @param vertex        EPGM vertex
     */
    JSONVertex(Vertex vertex) {
        this.id = vertex.getId().toString();
        this.data = new HashMap<>();
        for (Map.Entry<String,Object> entry : vertex.getProperties().entrySet()) {
            this.data.put(entry.getKey(), entry.getValue().toString());
        }
        this.meta = new Meta(vertex.getLabel(), vertex.getGraphs());
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String,String> getData() {
        return this.data;
    }

    public void setData(Map<String,String> data) {
        this.data = data;
    }

    public Meta getMeta() {
        return this.meta;
    }

    public void setMeta(Meta meta) {
        this.meta = meta;
    }

}
