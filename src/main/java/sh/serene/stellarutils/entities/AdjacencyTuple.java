package sh.serene.stellarutils.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AdjacencyTuple {

    public final Vertex source;
    public final Map<Edge,Vertex> inbound;
    public final Map<Edge,Vertex> outbound;

    public AdjacencyTuple(Vertex source, Map<Edge,Vertex> inbound, Map<Edge,Vertex> outbound) {
        this.source = source;
        this.inbound = new HashMap<>(inbound);
        this.outbound = new HashMap<>(outbound);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof AdjacencyTuple) {
            AdjacencyTuple tupleo = (AdjacencyTuple) other;
            return (
                    Objects.equals(this.source, tupleo.source) &&
                            Objects.equals(this.inbound, tupleo.inbound) &&
                            Objects.equals(this.outbound, tupleo.outbound)
            );
        } else {
            return false;
        }
    }

}
