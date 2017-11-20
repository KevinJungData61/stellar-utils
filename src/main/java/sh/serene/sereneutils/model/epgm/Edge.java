package sh.serene.sereneutils.model.epgm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * POJO Implementation of an EPGM Edge
 */
public class Edge extends Element implements Serializable {

    /**
     * Edge source identifier
     */
    private ElementId src;

    /**
     * Edge destination identifier
     */
    private ElementId dst;

    /**
     * Graphs that edge is contained in
     */
    private List<ElementId> graphs;

    private Edge(final ElementId id,
                final ElementId src,
                final ElementId dst,
                final Map<String,PropertyValue> properties,
                final String label,
                 final List<ElementId> graphs) {
        super(id, properties, label);
        this.src = src;
        this.dst = dst;
        this.graphs = graphs;
    }

    private Edge(final String id,
                 final String src,
                 final String dst,
                 final Map<String,PropertyValue> properties,
                 final String label,
                 final List<String> graphs) {
        super(id, properties, label);
        this.src = ElementId.fromString(src);
        this.dst = ElementId.fromString(dst);
        this.setGraphsFromStrings(graphs);
    }

    public Edge() {}

    /**
     * Creates an edge based on the given parameters
     *
     * @param id            edge identifier
     * @param src           source identifier
     * @param dst           destination identifier
     * @param properties    edge properties
     * @param label         edge label
     * @param graphs        graphs that edge is contained in
     * @return              new edge
     */
    public static Edge create(final ElementId id,
                              final ElementId src,
                              final ElementId dst,
                              final Map<String,PropertyValue> properties,
                              final String label,
                              final List<ElementId> graphs) {
        return new Edge(id, src, dst, properties, label, graphs);
    }

    /**
     * Creates an edge based on the given parameters
     *
     * @param id            edge identifier string
     * @param src           source identifier string
     * @param dst           destination identifier string
     * @param properties    edge properties
     * @param label         edge label
     * @param graphs        graphs that edge is contained in
     * @return              new edge
     */
    public static Edge createFromStringIds(final String id,
                              final String src,
                              final String dst,
                              final Map<String,PropertyValue> properties,
                              final String label,
                              final List<String> graphs) {
        return new Edge(id, src, dst, properties, label, graphs);
    }

    public ElementId getSrc() {
        return this.src;
    }

    public void setSrc(ElementId src) {
        this.src = src;
    }

    public ElementId getDst() {
        return this.dst;
    }

    public void setDst(ElementId dst) {
        this.dst = dst;
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
        return (obj instanceof Edge) && ((Edge) obj).getId().equals(this.id);
    }
}
