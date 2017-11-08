package au.data61.serene.sereneutils.core.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Vertex extends Element implements Serializable {

    private List<String> graphs;

    private Vertex(final String id,
                   final Map<String,String> properties,
                   final String label,
                   final List<String> graphs) {
        super(id, properties, label);
        this.graphs = graphs;
    }

    public static Vertex create(final String id,
                                final Map<String,String> properties,
                                final String label,
                                final List<String> graphs) {
        return new Vertex(id, properties, label, graphs);
    }

    public List<String> getGraphs() {
        return this.graphs;
    }

    public void setGraphs(List<String> graphs) {
        this.graphs = graphs;
    }
}
