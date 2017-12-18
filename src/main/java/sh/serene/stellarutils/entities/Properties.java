package sh.serene.stellarutils.entities;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to add properties of different types to
 *
 */
public class Properties implements Serializable {

    private Map<String,PropertyValue> map;

    private Properties() {
        this.map = new HashMap<>();
    }

    /**
     * Create properties
     *
     * @return  new properties
     */
    public static Properties create() {
        return new Properties();
    }

    /**
     * Get map
     *
     * @return  Map of (String, PropertyValue)
     */
    public Map<String,PropertyValue> getMap() {
        return new HashMap<>(this.map);
    }

    /**
     * Add a boolean property
     *
     * @param key       property key
     * @param value     boolean property value
     */
    public void add(String key, boolean value) {
        this.map.put(key, PropertyValue.create(value));
    }

    /**
     * Add an integer property
     *
     * @param key       property key
     * @param value     integer property value
     */
    public void add(String key, int value) {
        this.map.put(key, PropertyValue.create(value));
    }

    /**
     * Add a long property
     *
     * @param key       property key
     * @param value     long property value
     */
    public void add(String key, long value) {
        this.map.put(key, PropertyValue.create(value));
    }

    /**
     * Add a double property
     *
     * @param key       property key
     * @param value     double property value
     */
    public void add(String key, double value) {
        this.map.put(key, PropertyValue.create(value));
    }

    /**
     * Add a string property
     *
     * @param key       property key
     * @param value     string property value
     */
    public void add(String key, String value) {
        this.map.put(key, PropertyValue.create(value));
    }

}
