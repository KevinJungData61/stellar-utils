package sh.serene.sereneutils.model.epgm;

import sh.serene.sereneutils.model.common.Element;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.PropertyValue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POJO Implementation of an EPGM EPGMEdge
 */
public class EPGMEdge extends Element implements Serializable {

    /**
     * EPGMEdge source identifier
     */
    private ElementId src;

    /**
     * EPGMEdge destination identifier
     */
    private ElementId dst;

    /**
     * Graphs that edge is contained in
     */
    private List<ElementId> graphs;

    private EPGMEdge(final ElementId id,
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

    private EPGMEdge(final String id,
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

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public EPGMEdge() {}

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
    public static EPGMEdge create(final ElementId id,
                                  final ElementId src,
                                  final ElementId dst,
                                  final Map<String,PropertyValue> properties,
                                  final String label,
                                  final List<ElementId> graphs) {
        return new EPGMEdge(id, src, dst, properties, label, graphs);
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
    public static EPGMEdge createFromStringIds(final String id,
                                               final String src,
                                               final String dst,
                                               final Map<String,PropertyValue> properties,
                                               final String label,
                                               final List<String> graphs) {
        return new EPGMEdge(id, src, dst, properties, label, graphs);
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
        return (obj instanceof EPGMEdge) && ((EPGMEdge) obj).getId().equals(this.id);
    }

    public EPGMEdge copy(ElementId graphId) {
        ElementId id = ElementId.create();
        List<ElementId> graphs = new ArrayList<>(this.graphs);
        graphs.add(graphId);
        return new EPGMEdge(id, this.src, this.dst, this.properties, this.label, graphs);
    }

    public EPGMEdge copyWithProperty(ElementId graphId, String propertyKey, PropertyValue propertyValue) {
        ElementId id = ElementId.create();
        List<ElementId> graphs = new ArrayList<>(this.graphs);
        graphs.add(graphId);
        Map<String,PropertyValue> properties = new HashMap<>(this.properties);
        if (propertyValue == null) {
            properties.remove(propertyKey);
        } else {
            properties.put(propertyKey, propertyValue);
        }
        return new EPGMEdge(id, this.src, this.dst, properties, this.label, graphs);
    }

    public EPGMEdge copyWithLabel(ElementId graphId, String label) {
        ElementId id = ElementId.create();
        List<ElementId> graphs = new ArrayList<>(this.graphs);
        graphs.add(graphId);
        return new EPGMEdge(id, this.src, this.dst, this.properties, label, graphs);
    }
}
