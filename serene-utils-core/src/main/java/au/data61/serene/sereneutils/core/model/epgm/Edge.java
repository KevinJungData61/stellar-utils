package au.data61.serene.sereneutils.core.model.epgm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Edge extends Element implements Serializable {

    private ElementId src;
    private ElementId dst;
    private List<ElementId> graphs;

    private Edge(final String id,
                final String src,
                final String dst,
                final Map<String,Object> properties,
                final String label,
                 final List<String> graphs) {
        super(id, properties, label);
        this.src = ElementId.fromString(src);
        this.dst = ElementId.fromString(dst);
        this.setGraphs(graphs);
    }

    public static Edge create(final String id,
                              final String src,
                              final String dst,
                              final Map<String,Object> properties,
                              final String label,
                              final List<String> graphs) {
        return new Edge(id, src, dst, properties, label, graphs);
    }

    public ElementId getSrc() {
        return this.src;
    }

    public void setSrc(String src) {
        this.src = ElementId.fromString(src);
    }

    public void setSrc(ElementId src) {
        this.src = src;
    }

    public ElementId getDst() {
        return this.dst;
    }

    public void setDst(String dst) {
        this.dst = ElementId.fromString(dst);
    }

    public void setDst(ElementId dst) {
        this.dst = dst;
    }

    public List<ElementId> getGraphs() {
        return this.graphs;
    }

    public void setGraphs(List<String> graphs) {
        this.graphs = new ArrayList<>();
        for (String g : graphs) {
            this.graphs.add(ElementId.fromString(g));
        }
    }
}