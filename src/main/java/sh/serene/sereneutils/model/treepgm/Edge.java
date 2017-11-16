package sh.serene.sereneutils.model.treepgm;

import java.util.List;
import java.util.Map;

public class Edge extends Element {

    private String src;
    private String dst;
    private List<String> graphs;

    private Edge(final String id,
                 final String src,
                 final String dst,
                 final Map<String,String> properties,
                 final String label,
                 final List<String> graphs,
                 final boolean type) {
        super(id, properties, label, type);
        this.src = src;
        this.dst = dst;
        this.graphs = graphs;
    }

    public static Edge create(final String id,
                              final String src,
                              final String dst,
                              final Map<String,String> properties,
                              final String label,
                              final List<String> graphs,
                              final boolean type) {
        return new Edge(id, src, dst, properties, label, graphs, type);
    }

    public String getSrc() {
        return this.src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return this.dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public List<String> getGraphs() {
        return this.graphs;
    }

    public void setGraphs(List<String> graphs) {
        this.graphs = graphs;
    }
}
