package sh.serene.sereneutils.model.treepgm;

import java.util.Map;

public abstract class Element {

    protected String id;
    protected Map<String,String> properties;
    protected String label;
    protected boolean real;

    protected Element(String id, Map<String,String> properties, String label) {
        this(id, properties, label, false);
    }

    protected Element(String id, Map<String,String> properties, String label, boolean real) {
        this.id = id;
        this.properties = properties;
        this.label = label;
        this.real = real;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean getReal() {
        return this.real;
    }

    public void setReal(boolean real) {
        this.real = real;
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
}
