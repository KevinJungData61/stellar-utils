package sh.serene.stellarutils.graph.api;

import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.exceptions.InvalidIdException;

import java.util.Map;

/**
 * Stellar Graph Buffer
 */
public interface StellarGraphBuffer {

    /**
     * Add a vertex to the graph buffer
     *
     * @param label         vertex label
     * @param properties    vertex properties
     * @return              vertex id
     */
    ElementId addVertex(String label, Map<String,PropertyValue> properties);

    /**
     * Add an edge to the graph buffer
     *
     * @param label         edge label
     * @param src           edge source
     * @param dst           edge destination
     * @param properties    edge properties
     * @return              edge id
     * @throws InvalidIdException
     */
    ElementId addEdge(String label, ElementId src, ElementId dst, Map<String,PropertyValue> properties)
            throws InvalidIdException;

    /**
     * Get graph
     *
     * @return  graph
     */
    StellarGraph toGraph();
}
