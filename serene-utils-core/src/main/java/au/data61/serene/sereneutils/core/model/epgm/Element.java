package au.data61.serene.sereneutils.core.model.epgm;

import java.util.List;
import java.util.Map;

public abstract class Element {

    protected ElementId id;
    protected Map<String,Object> properties;
    protected String label;

    protected Element() {
        this.id = null;
        this.properties = null;
        this.label = null;
    }

    protected Element(String id, Map<String,Object> properties, String label) {
        this.id = ElementId.fromString(id);
        this.properties = properties;
        this.label = label;
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
