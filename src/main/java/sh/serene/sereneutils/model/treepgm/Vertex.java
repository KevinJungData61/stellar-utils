package sh.serene.sereneutils.model.treepgm;

import java.util.List;
import java.util.Map;

public class Vertex extends Element {

    private List<String> graphs;

    private Vertex(final String id,
                   final Map<String,String> properties,
                   final String label,
                   final List<String> graphs,
                   final boolean real) {
        super(id, properties, label, real);
        this.graphs = graphs;
    }

    public static Vertex create(final String id,
                                final Map<String,String> properties,
                                final String label,
                                final List<String> graphs,
                                final boolean real) {
        return new Vertex(id, properties, label, graphs, real);
    }

    public List<String> getGraphs() {
        return this.graphs;
    }

    public void setGraphs(List<String> graphs) {
        this.graphs = graphs;
    }

}
