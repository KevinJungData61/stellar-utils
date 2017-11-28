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
 * POJO Implementation of an EPGM EPGMVertex
 */
public class EPGMVertex extends Element implements Serializable {

    private List<ElementId> graphs;

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public EPGMVertex() { }

    private EPGMVertex(final ElementId id,
                       final Map<String,PropertyValue> properties,
                       final String label,
                       final List<ElementId> graphs) {
        super(id, properties, label);
        this.graphs = graphs;
    }

    private EPGMVertex(final String id,
                       final Map<String,PropertyValue> properties,
                       final String label,
                       final List<String> graphs) {
        super(id, properties, label);
        this.setGraphsFromStrings(graphs);
    }

    /**
     * Creates a vertex based on the given parameters
     *
     * @param id            vertex identifier
     * @param properties    vertex properties
     * @param label         vertex label
     * @param graphs        graphs that vertex is contained in
     * @returns             new vertex
     */
    public static EPGMVertex create(final ElementId id,
                                    final Map<String,PropertyValue> properties,
                                    final String label,
                                    final List<ElementId> graphs) {
        return new EPGMVertex(id, properties, label, graphs);
    }

    /**
     * Creates a vertex based on the given parameters
     *
     * @param id            vertex identifier string
     * @param properties    vertex properties
     * @param label         vertex label
     * @param graphs        graphs that vertex is contained in
     * @return              new vertex
     */
    public static EPGMVertex createFromStringIds(final String id,
                                                 final Map<String,PropertyValue> properties,
                                                 final String label,
                                                 final List<String> graphs) {
        return new EPGMVertex(id, properties, label, graphs);
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
        return (obj instanceof EPGMVertex) && ((EPGMVertex)obj).getId().equals(this.id);
    }

    public EPGMVertex copy(ElementId graphId) {
        ElementId id = ElementId.create();
        List<ElementId> graphs = new ArrayList<>(this.graphs);
        graphs.add(graphId);
        return new EPGMVertex(id, this.properties, this.label, graphs);
    }

    public EPGMVertex copyWithProperty(ElementId graphId, String propertyKey, PropertyValue propertyValue) {
        ElementId id = ElementId.create();
        List<ElementId> graphs = new ArrayList<>(this.graphs);
        graphs.add(graphId);
        Map<String,PropertyValue> properties = new HashMap<>(this.properties);
        if (propertyValue == null) {
            properties.remove(propertyKey);
        } else {
            properties.put(propertyKey, propertyValue);
        }
        return new EPGMVertex(id, properties, this.label, graphs);
    }

    public EPGMVertex copyWithLabel(final EPGMVertex vertex, ElementId graphId, String label) {
        ElementId id = ElementId.create();
        List<ElementId> graphs = new ArrayList<>(vertex.getGraphs());
        graphs.add(graphId);
        return new EPGMVertex(id, vertex.getProperties(), label, graphs);
    }

}
