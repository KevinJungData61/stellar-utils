package sh.serene.sereneutils.model.epgm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * POJO Implementation of an EPGM Vertex
 */
public class Vertex extends Element implements Serializable {

    private List<ElementId> graphs;

    public Vertex() { }


    private Vertex(final ElementId id,
                   final Map<String,PropertyValue> properties,
                   final String label,
                   final List<ElementId> graphs) {
        super(id, properties, label);
        this.graphs = graphs;
    }

    private Vertex(final String id,
                   final Map<String,PropertyValue> properties,
                   final String label,
                   final List<String> graphs) {
        super(id, properties, label);
        this.setGraphsFromStrings(graphs);
    }

    /**
     * Creates a vertex based on the given parameters
     *
     * @param id            vertex identifier string
     * @param properties    vertex properties
     * @param label         vertex label
     * @param graphs        graphs that vertex is contained in
     * @returns             new vertex
     */
    public static Vertex create(final ElementId id,
                                final Map<String,PropertyValue> properties,
                                final String label,
                                final List<ElementId> graphs) {
        return new Vertex(id, properties, label, graphs);
    }

    public static Vertex createFromStringIds(final String id,
                                final Map<String,PropertyValue> properties,
                                final String label,
                                final List<String> graphs) {
        return new Vertex(id, properties, label, graphs);
    }

    public List<ElementId> getGraphs() {
        return this.graphs;
    }

    public void setGraphs(List<ElementId> graphs) {
        this.graphs = graphs;
    }

    private void setGraphsFromStrings(List<String> graphs) {
        this.graphs = new ArrayList<>();
        for (String g : graphs) {
            this.graphs.add(ElementId.fromString(g));
        }
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Vertex) && ((Vertex)obj).getId().equals(this.id);
    }

}
