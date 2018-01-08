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
     * @param name          unique vertex name
     * @param label         vertex label
     * @param properties    vertex properties
     * @return              vertex id
     */
    ElementId addVertex(String name, String label, Map<String,PropertyValue> properties);

    /**
     * Add an edge to the graph buffer
     *
     * @param name          unique edge name
     * @param src           edge source
     * @param dst           edge destination
     * @param label         edge label
     * @param properties    edge properties
     * @return              edge id
     */
    ElementId addEdge(String name, String src, String dst, String label, Map<String,PropertyValue> properties);

    /**
     * Get graph
     *
     * @return  graph
     */
    StellarGraph toGraph();
}
