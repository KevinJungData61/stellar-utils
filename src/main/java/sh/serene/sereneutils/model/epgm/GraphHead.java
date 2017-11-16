package sh.serene.sereneutils.model.epgm;

import java.io.Serializable;
import java.util.Map;

/**
 * POJO Implementation of an EPGM GraphHead
 */
public class GraphHead extends Element implements Serializable {

    private GraphHead(final String id, final Map<String,Object> properties, final String label) {
        super(id, properties, label);
    }

    /**
     * Creates a new graph head based on given parameters.
     *
     * @param id            graph head identifier string
     * @param properties    graph head properties
     * @param label         graph head label
     * @return              new graph head
     */
    public static GraphHead create(final String id, final Map<String,Object> properties, final String label) {
        return new GraphHead(id, properties, label);
    }
}
