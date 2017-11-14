package au.data61.serene.sereneutils.core.io.json;

import au.data61.serene.sereneutils.core.model.epgm.GraphHead;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class JSONGraphHead implements Serializable {

    private String id;
    private Map<String,String> data;
    private Meta meta;

    public static class Meta {
        private String label;

        public Meta() { }

        public Meta(String label) {
            this.label = label;
        }

        public String getLabel() {
            return this.label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

    }

    public JSONGraphHead() { }

    public JSONGraphHead(GraphHead graphHead) {
        this.id = graphHead.getId().toString();
        this.data = new HashMap<>();
        for (Map.Entry<String,Object> entry : graphHead.getProperties().entrySet()) {
            this.data.put(entry.getKey(), entry.getValue().toString());
        }
        this.meta = new Meta(graphHead.getLabel());
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String,String> getData() {
        return this.data;
    }

    public void setData(Map<String,String> data) {
        this.data = data;
    }

    public Meta getMeta() {
        return this.meta;
    }

    public void setMeta(Meta meta) {
        this.meta = meta;
    }

}
