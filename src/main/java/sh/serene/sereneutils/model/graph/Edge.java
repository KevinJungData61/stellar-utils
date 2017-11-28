package sh.serene.sereneutils.model.graph;

import sh.serene.sereneutils.model.common.Element;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.PropertyValue;
import sh.serene.sereneutils.model.epgm.EPGMEdge;

import java.util.Map;

public class Edge extends Element {

    private ElementId src;
    private ElementId dst;

    public Edge() {}

    private Edge(final ElementId id,
                 final ElementId src,
                 final ElementId dst,
                 final Map<String,PropertyValue> properties,
                 final String label) {
        super(id, properties, label);
        this.src = src;
        this.dst = dst;
    }

    public static Edge fromEPGM(EPGMEdge epgmEdge) {
        return new Edge(epgmEdge.getId(), epgmEdge.getSrc(),
                epgmEdge.getDst(), epgmEdge.getProperties(), epgmEdge.getLabel());
    }

    public void setSrc(ElementId src) {
        this.src = src;
    }

    public ElementId getSrc() {
        return this.src;
    }

    public void setDst(ElementId dst) {
        this.dst = dst;
    }

    public ElementId getDst() {
        return this.dst;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof  Edge) && ((Edge) obj).getId().equals(this.id);
    }
}
