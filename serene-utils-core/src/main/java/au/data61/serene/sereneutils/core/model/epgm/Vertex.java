package au.data61.serene.sereneutils.core.model.epgm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * POJO Implementation of an EPGM Vertex
 */
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

    /**
     * Creates a vertex based on the given parameters
     *
     * @param id            vertex identifier string
     * @param properties    vertex properties
     * @param label         vertex label
     * @param graphs        graphs that vertex is contained in
     * @returns             new vertex
     */
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
