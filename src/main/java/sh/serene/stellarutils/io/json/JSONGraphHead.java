package sh.serene.stellarutils.io.json;

import sh.serene.stellarutils.model.epgm.GraphHead;
import sh.serene.stellarutils.model.epgm.PropertyValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * EPGM GraphHead with fields that can be serialised in json format
 */
public class JSONGraphHead implements Serializable {

    private String id;
    private Map<String,String> data;
    private Meta meta;

    public static class Meta {
        private String label;

        public Meta() { }

        Meta(String label) {
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

    /**
     * Creates a new json graph head from an EPGM graph head
     *
     * @param graphHead     EPGM graph head
     */
    JSONGraphHead(GraphHead graphHead) {
        this.id = graphHead.getId().toString();
        this.data = new HashMap<>();
        for (Map.Entry<String,PropertyValue> entry : graphHead.getProperties().entrySet()) {
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
