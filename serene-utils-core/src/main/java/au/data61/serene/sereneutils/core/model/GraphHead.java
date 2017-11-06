package au.data61.serene.sereneutils.core.model;

import java.io.Serializable;
import java.util.Map;

public class GraphHead extends Element implements Serializable {

    private GraphHead(final String id, final Map<String,Object> properties, final String label) {
        super(id, properties, label);
    }

    public static GraphHead create(final String id, final Map<String,Object> properties, final String label) {
        return new GraphHead(id, properties, label);
    }
}
