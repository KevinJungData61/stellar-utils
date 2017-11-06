package au.data61.serene.sereneutils.core.model;

import java.util.List;
import java.util.Map;

public abstract class Element {

    protected String id;
    protected Map<String,Object> properties;
    protected String label;

    protected Element(String id, Map<String,Object> properties, String label) {
        this.id = id;
        this.properties = properties;
        this.label = label;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String,Object> getProperties() {
        return this.properties;
    }

    public void setProperties(Map<String,Object> properties) {
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
