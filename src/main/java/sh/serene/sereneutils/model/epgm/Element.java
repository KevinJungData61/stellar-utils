package sh.serene.sereneutils.model.epgm;

import java.util.Map;

/**
 * Abstract base class for graph heads, vertices, and edges.
 *
 */
public abstract class Element {

    /**
     * Element identifier
     */
    protected ElementId id;

    /**
     * Element properties
     */
    protected Map<String,PropertyValue> properties;

    /**
     * Element label
     */
    protected String label;

    /**
     * Default constructor
     */
    protected Element() { }

    /**
     * Creates an element from the given parameters
     *
     * @param id            element identifier
     * @param properties    element properties
     * @param label         element label
     */
    protected Element(ElementId id, Map<String,PropertyValue> properties, String label) {
        this.id = id;
        this.properties = properties;
        this.label = label;
    }

    /**
     * Creates an element from the given parameters
     *
     * @param id            element identifier string
     * @param properties    element properties
     * @param label         element label
     */
    protected Element(String id, Map<String,PropertyValue> properties, String label) {
        this(ElementId.fromString(id), properties, label);
    }

    public ElementId getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = ElementId.fromString(id);
    }

    public void setId(ElementId id) {
        this.id = id;
    }

    public Map<String,PropertyValue> getProperties() {
        return this.properties;
    }

    public void setProperties(Map<String,PropertyValue> properties) {
        this.properties = properties;
    }

    public String getLabel() {
        return this.label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

}
