package sh.serene.stellarutils.graph.api;

import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.GraphHead;
import sh.serene.stellarutils.entities.Vertex;


public interface StellarGraph {

    /**
     * Get graph head
     *
     * @return  graph head
     */
    GraphHead getGraphHead();

    /**
     * Get vertices
     *
     * @return  vertices
     */
    StellarGraphMemory<Vertex> getVertices();

    /**
     * Get edges
     *
     * @return  edges
     */
    StellarGraphMemory<Edge> getEdges();

    /**
     * Union two stellar graphs
     *
     * @param other     other graph
     * @return          union of graphs
     */
    StellarGraph union(StellarGraph other);
}
