package sh.serene.stellarutils.entities;

import java.util.Map;

/**
 * Abstract base class for graph heads, vertices, and edges.
 *
 */
public interface Element {

    /**
     * Get element identifier
     *
     * @return  element ID
     */
    ElementId getId();

    /**
     * Set element identifier
     *
     * @param id    element ID
     */
    void setId(ElementId id);

    /**
     * Get element properties
     *
     * @return  element properties
     */
    Map<String,PropertyValue> getProperties();

    /**
     * Set element properties
     *
     * @param properties    element properties
     */
    void setProperties(Map<String,PropertyValue> properties);

    /**
     * Get element label
     *
     * @return  element label
     */
    String getLabel();

    /**
     * Set element label
     *
     * @param label element label
     */
    void setLabel(String label);

    /**
     * Get version
     *
     * @return  version ID
     */
    ElementId version();

    /**
     * Get element property
     *
     * @param key   property key
     * @return      property value
     */
    PropertyValue getProperty(String key);

    /**
     * Get element property value object
     *
     * @param key   property key
     * @return      property value object
     */
    Object getPropertyValue(String key);

    /**
     * Get element property value object of type
     *
     * @param key   property key
     * @param type  property value type class
     * @param <T>   type
     * @return      property value object of type T
     */
    <T> T getPropertyValue(String key, Class<T> type);

    boolean equals(Object obj);

}
