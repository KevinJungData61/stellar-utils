package sh.serene.sereneutils.model.epgm;

import java.io.Serializable;
import java.util.Map;

public class Vertex implements Element, Serializable, Cloneable {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;

    public Vertex() {}

    private Vertex(final ElementId id,
                   final Map<String,PropertyValue> properties,
                   final String label) {
        this.id = id;
        this.properties = properties;
        this.label = label;
    }

    /**
     * Copy constructor
     *
     * @param vertex
     */
    public Vertex(Vertex vertex) {
        this(vertex.getId(), vertex.getProperties(), vertex.getLabel());
    }

    public Vertex clone() {
        return new Vertex(this);
    }

    public static Vertex fromEPGM(VertexCollection vertexCollection) {
        return new Vertex(vertexCollection.getId(), vertexCollection.getProperties(), vertexCollection.getLabel());
    }

    public static Vertex create(final ElementId id,
                                final Map<String,PropertyValue> properties,
                                final String label) {
        return new Vertex(id, properties, label);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Vertex) && ((Vertex) obj).getId().equals(this.id);
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
