package sh.serene.stellarutils.entities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * POJO Implementation of an EPGM EdgeCollection
 */
public class Edge implements Element, Serializable, Cloneable {

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
    private ElementId version;

    private Edge(
            final ElementId id,
            final ElementId src,
            final ElementId dst,
            final Map<String,PropertyValue> properties,
            final String label,
            final ElementId version
    ) {
        if (id == null) {
            throw new NullPointerException("ID was null");
        } else if (src == null) {
            throw new NullPointerException("Source ID was null");
        } else if (dst == null) {
            throw new NullPointerException("Target ID was null");
        } else if (version == null) {
            throw new NullPointerException("Version was null");
        }
        this.id = id;
        this.src = src;
        this.dst = dst;
        this.properties = (properties == null) ? new HashMap<>() : properties;
        this.label = (label == null) ? "" : label;
        this.version = version;
    }

    private Edge(
            final String id,
            final String src,
            final String dst,
            final Map<String,PropertyValue> properties,
            final String label,
            final String version
    ) {
        this.id = ElementId.fromString(id);
        this.src = ElementId.fromString(src);
        this.dst = ElementId.fromString(dst);
        this.properties = (properties == null) ? new HashMap<>() : properties;
        this.label = (label == null) ? "" : label;
        this.version = ElementId.fromString(version);
    }

    /**
     * Copy constructor
     *
     * @param edge
     */
    public Edge(Edge edge) {
        this(
                edge.getId(),
                edge.getSrc(),
                edge.getDst(),
                edge.getProperties(),
                edge.getLabel(),
                edge.version()
        );
    }

    public Edge clone() {
        return new Edge(this);
    }

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public Edge() {}

    /**
     * Creates an edge based on the given parameters
     *
     * @param id            edge identifier
     * @param src           source identifier
     * @param dst           destination identifier
     * @param properties    edge properties
     * @param label         edge label
     * @param version       id of first graph this version of the edge was contained in
     * @return              new edge
     */
    public static Edge create(
            final ElementId id,
            final ElementId src,
            final ElementId dst,
            final Map<String,PropertyValue> properties,
            final String label,
            final ElementId version
    ) {
        return new Edge(id, src, dst, properties, label, version);
    }

    /**
     * Creates an edge based on the given parameters. A unique ID is generated.
     *
     * @param src           source identifier
     * @param dst           destination identifier
     * @param properties    edge properties
     * @param label         edge label
     * @return              new edge
     */
    public static Edge create(
            final ElementId src,
            final ElementId dst,
            final Map<String,PropertyValue> properties,
            final String label,
            final ElementId version
    ) {
        return new Edge(ElementId.create(), src, dst, properties, label, version);
    }

    /**
     * Creates an edge based on the given parameters
     *
     * @param id            edge identifier string
     * @param src           source identifier string
     * @param dst           destination identifier string
     * @param properties    edge properties
     * @param label         edge label
     * @param version       id of first graph this version of the edge was contained in
     * @return              new edge
     */
    public static Edge createFromStringIds(
            final String id,
            final String src,
            final String dst,
            final Map<String,PropertyValue> properties,
            final String label,
            final String version
    ) {
        return new Edge(id, src, dst, properties, label, version);
    }

    /**
     * Create an edge from an edge collection
     *
     * @param edgeCollection    edge collection
     * @return                  new edge
     */
    public static Edge createFromCollection(EdgeCollection edgeCollection) {
        return new Edge(
                edgeCollection.getId(),
                edgeCollection.getSrc(),
                edgeCollection.getDst(),
                edgeCollection.getProperties(),
                edgeCollection.getLabel(),
                edgeCollection.version()
        );
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
    public boolean equals(Object obj) {
        if (obj instanceof Edge) {
            Edge other = (Edge) obj;
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

    @Override
    public ElementId version() {
        return this.version;
    }

    public ElementId getVersion() {
        return version();
    }

    public void setVersion(ElementId version) {
        this.version = version;
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
