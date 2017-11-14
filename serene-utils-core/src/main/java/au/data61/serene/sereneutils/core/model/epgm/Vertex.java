package au.data61.serene.sereneutils.core.model.epgm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Vertex extends Element implements Serializable {

    private List<ElementId> graphs;

    public Vertex() {
        super();
        this.graphs = null;
    }

    private Vertex(final String id,
                   final Map<String,Object> properties,
                   final String label,
                   final List<String> graphs) {
        super(id, properties, label);
        this.setGraphs(graphs);
    }

    public static Vertex create(final String id,
                                final Map<String,Object> properties,
                                final String label,
                                final List<String> graphs) {
        return new Vertex(id, properties, label, graphs);
    }

    public List<ElementId> getGraphs() {
        return this.graphs;
    }

    public void setGraphs(List<String> graphs) {
        this.graphs = new ArrayList<>();
        for (String g : graphs) {
            this.graphs.add(ElementId.fromString(g));
        }
    }

}
