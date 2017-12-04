package sh.serene.stellarutils.model.epgm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * POJO Implementation of an EPGM EdgeCollection
 */
public class EdgeCollection implements Element, Serializable, Cloneable {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;

    /**
     * EdgeCollection source identifier
     */
    private ElementId src;

    /**
     * EdgeCollection destination identifier
     */
    private ElementId dst;

    /**
     * Graphs that edge is contained in
     */
    private List<ElementId> graphs;

    private EdgeCollection(final ElementId id,
                           final ElementId src,
                           final ElementId dst,
                           final Map<String,PropertyValue> properties,
                           final String label,
                           final List<ElementId> graphs) {
        this.id = id;
        this.src = src;
        this.dst = dst;
        this.properties = properties;
        this.label = label;
        this.graphs = graphs;
    }

    private EdgeCollection(final String id,
                           final String src,
                           final String dst,
                           final Map<String,PropertyValue> properties,
                           final String label,
                           final List<String> graphs) {
        this.id = ElementId.fromString(id);
        this.src = ElementId.fromString(src);
        this.dst = ElementId.fromString(dst);
        this.properties = properties;
        this.label = label;
        this.setGraphsFromStrings(graphs);
    }

    /**
     * Copy constructor
     *
     * @param edge
     */
    public EdgeCollection(EdgeCollection edge) {
        this(
                edge.getId(),
                edge.getSrc(),
                edge.getDst(),
                edge.getProperties(),
                edge.getLabel(),
                edge.getGraphs()
        );
    }

    public EdgeCollection clone() {
        return new EdgeCollection(this);
    }

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public EdgeCollection() {}

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
    public static EdgeCollection create(final ElementId id,
                                        final ElementId src,
                                        final ElementId dst,
                                        final Map<String,PropertyValue> properties,
                                        final String label,
                                        final List<ElementId> graphs) {
        return new EdgeCollection(id, src, dst, properties, label, graphs);
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
    public static EdgeCollection createFromStringIds(final String id,
                                                     final String src,
                                                     final String dst,
                                                     final Map<String,PropertyValue> properties,
                                                     final String label,
                                                     final List<String> graphs) {
        return new EdgeCollection(id, src, dst, properties, label, graphs);
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

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EdgeCollection) {
            EdgeCollection other = (EdgeCollection) obj;
            return (this.id.equals(other.getId())
                    && this.src.equals(other.getSrc())
                    && this.dst.equals(other.getDst())
                    && this.properties.equals(other.getProperties())
                    && this.label.equals(other.getLabel())
            );
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    /**
     * Get element identifier
     *
     * @return element ID
     */
    @Override
    public ElementId getId() {
        return this.id;
    }

    /**
     * Set element identifier
     *
     * @param id element ID
     */
    @Override
    public void setId(ElementId id) {
        this.id = id;
    }

    /**
     * Get element properties
     *
     * @return element properties
     */
    @Override
    public Map<String, PropertyValue> getProperties() {
        return this.properties;
    }

    /**
     * Set element properties
     *
     * @param properties element properties
     */
    @Override
    public void setProperties(Map<String, PropertyValue> properties) {
        this.properties = properties;
    }

    /**
     * Get element label
     *
     * @return element label
     */
    @Override
    public String getLabel() {
        return this.label;
    }

    /**
     * Set element label
     *
     * @param label element label
     */
    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * Get element property
     *
     * @param key property key
     * @return property value
     */
    @Override
    public PropertyValue getProperty(String key) {
        return this.properties.get(key);
    }

    /**
     * Get element property value object
     *
     * @param key property key
     * @return property value object
     */
    @Override
    public Object getPropertyValue(String key) {
        PropertyValue pv = this.getProperty(key);
        return (pv == null) ? null : pv.value();
    }

    /**
     * Get element property value object of type
     *
     * @param key  property key
     * @param type property value type class
     * @return property value object of type T
     */
    @Override
    public <T> T getPropertyValue(String key, Class<T> type) {
        PropertyValue pv = this.getProperty(key);
        return (pv == null) ? null : pv.value(type);
    }
}
