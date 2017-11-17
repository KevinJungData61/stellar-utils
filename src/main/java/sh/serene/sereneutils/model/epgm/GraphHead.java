package sh.serene.sereneutils.model.epgm;

import java.io.Serializable;
import java.util.Map;

/**
 * POJO Implementation of an EPGM GraphHead
 */
public class GraphHead extends Element implements Serializable {

    private GraphHead(final ElementId id, final Map<String,PropertyValue> properties, final String label) {
        super(id, properties, label);
    }

    public GraphHead() {}

    /**
     * Creates a new graph head based on given parameters.
     *
     * @param id            graph head identifier
     * @param properties    graph head properties
     * @param label         graph head label
     * @return              new graph head
     */
    public static GraphHead create(final ElementId id, final Map<String,PropertyValue> properties, final String label) {
        return new GraphHead(id, properties, label);
    }

    public static GraphHead create(final String id, final Map<String,PropertyValue> properties, final String label) {
        return new GraphHead(ElementId.fromString(id), properties, label);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof GraphHead) && ((GraphHead)obj).getId().equals(this.id);
    }
}
