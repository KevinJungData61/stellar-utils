package sh.serene.sereneutils.model.epgm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * POJO Implementation of an EPGM VertexCollection
 */
public class VertexCollection implements Element, Serializable, Cloneable {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;
    private List<ElementId> graphs;

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public VertexCollection() { }

    private VertexCollection(final ElementId id,
                             final Map<String,PropertyValue> properties,
                             final String label,
                             final List<ElementId> graphs) {
        this.id = id;
        this.properties = properties;
        this.label = label;
        this.graphs = graphs;
    }

    private VertexCollection(final String id,
                             final Map<String,PropertyValue> properties,
                             final String label,
                             final List<String> graphs) {
        this.id = ElementId.fromString(id);
        this.properties = properties;
        this.label = label;
        this.setGraphsFromStrings(graphs);
    }

    /**
     * Copy constructor
     *
     * @param vertex
     */
    public VertexCollection(VertexCollection vertex) {
        this(vertex.getId(), vertex.getProperties(), vertex.getLabel(), vertex.getGraphs());
    }

    public VertexCollection clone() {
        return new VertexCollection(this);
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
    public static VertexCollection create(final ElementId id,
                                          final Map<String,PropertyValue> properties,
                                          final String label,
                                          final List<ElementId> graphs) {
        return new VertexCollection(id, properties, label, graphs);
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
    public static VertexCollection createFromStringIds(final String id,
                                                       final Map<String,PropertyValue> properties,
                                                       final String label,
                                                       final List<String> graphs) {
        return new VertexCollection(id, properties, label, graphs);
    }

    @Override
    public ElementId getId() {
        return this.id;
    }

    @Override
    public void setId(ElementId id) {
        this.id = id;
    }

    @Override
    public Map<String,PropertyValue> getProperties() {
        return this.properties;
    }

    @Override
    public void setProperties(Map<String,PropertyValue> properties) {
        this.properties = properties;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public PropertyValue getProperty(String key) {
        return this.properties.get(key);
    }

    @Override
    public Object getPropertyValue(String key) {
        PropertyValue pv = this.getProperty(key);
        return (pv == null) ? null : pv.value();
    }

    @Override
    public <T> T getPropertyValue(String key, Class<T> type) {
        PropertyValue pv = this.getProperty(key);
        return (pv == null) ? null : pv.value(type);
    }

    @Override
    public List<ElementId> getGraphs() {
        return this.graphs;
    }

    @Override
    public void setGraphs(List<ElementId> graphs) {
        this.graphs = graphs;
    }

    private void setGraphsFromStrings(List<String> graphs) {
        this.graphs = new ArrayList<>();
        for (String g : graphs) {
            this.graphs.add(ElementId.fromString(g));
        }
    }

    public VertexCollection addToGraphs(List<ElementId> graphs) {
        List<ElementId> graphsNew = new ArrayList<>(this.graphs);
        graphsNew.addAll(graphs);
        return new VertexCollection(this.id, this.properties, this.label, graphsNew);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VertexCollection) {
            VertexCollection other = (VertexCollection) obj;
            return ((this.id.equals(other.getId()))
                    && (this.properties.equals(other.getProperties()))
                    && (this.label.equals(other.getLabel())));
        } else {
            return false;
        }
    }

}
