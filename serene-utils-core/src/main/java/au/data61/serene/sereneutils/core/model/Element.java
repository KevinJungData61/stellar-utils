package au.data61.serene.sereneutils.core.model;

import java.util.List;
import java.util.Map;

public abstract class Element {

    protected String id;
    protected Map<String,String> properties;
    protected String label;

    protected Element(String id, Map<String,String> properties, String label) {
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

    public Map<String,String> getProperties() {
        return this.properties;
    }

    public void setProperties(Map<String,String> properties) {
        this.properties = properties;
    }

    public String getLabel() {
        return this.label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getProperty(String key) {
        return properties.get(key);
    }
}
