package sh.serene.sereneutils.model.graph;

import sh.serene.sereneutils.model.common.Element;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.PropertyValue;
import sh.serene.sereneutils.model.epgm.EPGMVertex;

import java.util.Map;

public class Vertex extends Element{

    public Vertex() {}

    private Vertex(final ElementId id,
                   final Map<String,PropertyValue> properties,
                   final String label) {
        super(id, properties, label);
    }

    public static Vertex fromEPGM(EPGMVertex epgmVertex) {
        return new Vertex(epgmVertex.getId(), epgmVertex.getProperties(), epgmVertex.getLabel());
    }

    public static Vertex create(final ElementId id,
                                final Map<String,PropertyValue> properties,
                                final String label) {
        return new Vertex(id, properties, label);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Vertex) && ((Vertex) obj).getId().equals(this.id);
    }
}
