package sh.serene.sereneutils.model.treepgm;

import java.io.Serializable;
import java.util.Map;

public class GraphHead extends Element implements Serializable {

    private GraphHead(final String id, final Map<String,String> properties, final String label) {
        super(id, properties, label);
    }

    public static GraphHead create(final String id, final Map<String,String> properties, final String label) {
        return new GraphHead(id, properties, label);
    }
}
