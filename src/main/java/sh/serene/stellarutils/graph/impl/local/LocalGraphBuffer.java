package sh.serene.stellarutils.graph.impl.local;

import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.exceptions.InvalidIdException;
import sh.serene.stellarutils.graph.api.StellarGraphBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a mutable object to create a local graph with
 */
public class LocalGraphBuffer implements StellarGraphBuffer {

    private GraphHead graphHead;
    private Map<ElementId,Vertex> vertices;
    private Map<ElementId,Edge> edges;

    public LocalGraphBuffer(String label, Map<String,PropertyValue> properties) {
        this.graphHead = GraphHead.create(ElementId.create(), properties, label);
        this.vertices = new HashMap<>();
        this.edges = new HashMap<>();
    }

    /**
     * Add a vertex to the graph buffer
     *
     * @param label      vertex label
     * @param properties vertex properties
     * @return vertex id
     */
    @Override
    public ElementId addVertex(String label, Map<String, PropertyValue> properties) {
        ElementId id = ElementId.create();
        vertices.put(id, Vertex.create(id, properties, label, this.graphHead.getId()));
        return id;
    }

    /**
     * Add an edge to the graph buffer
     *
     * @param label      edge label
     * @param src        edge source
     * @param dst        edge destination
     * @param properties edge properties
     * @return edge id
     * @throws InvalidIdException
     */
    @Override
    public ElementId addEdge(String label, ElementId src, ElementId dst, Map<String, PropertyValue> properties) throws InvalidIdException {
        if (!vertices.containsKey(src)) {
            throw new InvalidIdException("Error creating edge: source doesn't exist");
        } else if (!vertices.containsKey(dst)) {
            throw new InvalidIdException("Error creating edge: destination doesn't exist");
        } else {
            ElementId id = ElementId.create();
            edges.put(id, Edge.create(id, src, dst, properties, label, this.graphHead.getId()));
            return id;
        }
    }

    /**
     * Get graph
     *
     * @return graph
     */
    @Override
    public LocalGraph toGraph() {
        return new LocalGraph(
                this.graphHead,
                new ArrayList<>(this.vertices.values()),
                new ArrayList<>(this.edges.values())
        );
    }
}
