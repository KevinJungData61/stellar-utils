package sh.serene.stellarutils.graph.api;

import scala.Tuple2;
import sh.serene.stellarutils.entities.*;

import java.util.List;
import java.util.function.Predicate;


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

    /**
     * Get a list of weakly connected components
     *
     * @return  list of graphs
     */
    List<StellarGraph> getConnectedComponents();

    /**
     * Get a list of tuples containing vertices and their neighbours
     *
     * @return  list of adjacency tuples containing (souce, inbound, outbound)
     */
    List<AdjacencyTuple> getAdjacencyTuples(Predicate<Vertex> vertexPredicate);
}
