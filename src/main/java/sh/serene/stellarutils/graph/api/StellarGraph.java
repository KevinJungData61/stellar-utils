package sh.serene.stellarutils.graph.api;

import scala.Tuple2;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.ElementId;
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

    StellarGraphCollection toCollection();

    /**
     * Union two stellar graphs
     *
     * @param other     other graph
     * @return          union of graphs
     */
    StellarGraph union(StellarGraph other);

    /**
     * Union a set of vertices into current graph
     *
     * @param vertices  vertices
     * @return          new graph
     */
    StellarGraph unionVertices(StellarGraphMemory<Vertex> vertices);

    /**
     * Union a set of edges into current graph
     *
     * @param edges edges
     * @return      new graph
     */
    StellarGraph unionEdges(StellarGraphMemory<Edge> edges);

    StellarGraph union(StellarVertexMemory vertexMemory);

    StellarGraph union(StellarEdgeMemory edgeMemory);

    /**
     * Get edge list
     *
     * @return  edge list
     */
    StellarGraphMemory<Tuple2<ElementId,ElementId>> getEdgeList();
}
