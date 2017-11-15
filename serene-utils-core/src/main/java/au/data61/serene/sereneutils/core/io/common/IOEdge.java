package au.data61.serene.sereneutils.core.io.common;

import au.data61.serene.sereneutils.core.model.epgm.Edge;
import au.data61.serene.sereneutils.core.model.epgm.ElementId;
import au.data61.serene.sereneutils.core.model.epgm.Vertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * EPGM Edge with fields that can be serialised in json format
 */
public class IOEdge implements Serializable {

    private String id;
    private String source;
    private String target;
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

    public IOEdge() { }

    /**
     * Create a new json edge from EPGM edge
     *
     * @param edge  EPGM edge
     */
    IOEdge(Edge edge) {
        this.id = edge.getId().toString();
        this.source = edge.getSrc().toString();
        this.target = edge.getDst().toString();
        this.data = new HashMap<>();
        for (Map.Entry<String,Object> entry : edge.getProperties().entrySet()) {
            this.data.put(entry.getKey(), entry.getValue().toString());
        }
        this.meta = new Meta(edge.getLabel(), edge.getGraphs());
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSource() {
        return this.source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return this.target;
    }

    public void setTarget(String target) {
        this.target = target;
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
