package sh.serene.stellarutils.model.epgm;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * POJO Implementation of an EPGM GraphHead
 */
public class GraphHead implements Element, Serializable, Cloneable {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;

    private GraphHead(final ElementId id, final Map<String,PropertyValue> properties, final String label) {
        this.id = id;
        this.properties = properties;
        this.label = label;
    }

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public GraphHead() {}

    /**
     * Copy constructor
     *
     * @param graphHead
     */
    public GraphHead(GraphHead graphHead) {
        this(graphHead.getId(), graphHead.getProperties(), graphHead.getLabel());
    }

    public GraphHead clone() {
        return new GraphHead(this);
    }

    /**
     * Creates a new graph head based on given parameters.
     *
     * @param id            graph head identifier
     * @param properties    graph head properties
     * @param label         graph head label
     * @return              new graph head
     */
    public static GraphHead create(final ElementId id, final Map<String,PropertyValue> properties, final String label) {
        return new GraphHead(id, properties, label);
    }

    /**
     * Creates a new graph head based on given parameters.
     *
     * @param id            graph head identifier string
     * @param properties    graph head properties
     * @param label         graph head label
     * @return              new graph head
     */
    public static GraphHead createFromStringIds(final String id, final Map<String,PropertyValue> properties, final String label) {
        return new GraphHead(ElementId.fromString(id), properties, label);
    }

    /**
     * Create a new graph head with same properties and label
     *
     * @return  new graph head
     */
    public GraphHead copy() {
        ElementId graphId = ElementId.create();
        return new GraphHead(graphId, this.properties, this.label);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GraphHead) {
            GraphHead other = (GraphHead) obj;
            return ((this.id.equals(other.getId()))
                    && (this.properties.equals(other.getProperties()))
                    && (this.label.equals(other.getLabel()))
            );
        } else {
            return false;
        }
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
     * Get version
     *
     * @return version ID
     */
    @Override
    public ElementId version() {
        return this.id;
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
