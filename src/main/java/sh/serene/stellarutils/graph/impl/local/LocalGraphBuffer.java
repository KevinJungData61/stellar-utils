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
    private Map<String,ElementId> vertexIds;
    private Map<String,ElementId> edgeIds;

    public LocalGraphBuffer(String label, Map<String,PropertyValue> properties) {
        this.graphHead = GraphHead.create(ElementId.create(), properties, label);
        this.vertices = new HashMap<>();
        this.edges = new HashMap<>();
        this.vertexIds = new HashMap<>();
        this.edgeIds = new HashMap<>();
    }

    /**
     * Add a vertex to the graph buffer
     *
     * @param name       vertex unique name
     * @param label      vertex label
     * @param properties vertex properties
     * @return vertex id
     */
    @Override
    public ElementId addVertex(String name, String label, Map<String, PropertyValue> properties) {
        ElementId id = ElementId.create();
        if (vertexIds.containsKey(name)) {
            throw new InvalidIdException("A vertex with the same unique name already exists within graph buffer");
        } else {
            vertexIds.put(name, id);
        }
        vertices.put(id, Vertex.create(id, properties, label, this.graphHead.getId()));

        return id;
    }

    /**
     * Add an edge to the graph buffer
     *
     * @param name       edge unique name
     * @param src        edge source
     * @param dst        edge destination
     * @param label      edge label
     * @param properties edge properties
     * @return edge id
     */
    @Override
    public ElementId addEdge(
            String name, String src, String dst, String label, Map<String, PropertyValue> properties
    ) {
        if (!vertexIds.containsKey(src)) {
            throw new InvalidIdException("Error creating edge: source doesn't exist");
        } else if (!vertexIds.containsKey(dst)) {
            throw new InvalidIdException("Error creating edge: destination doesn't exist");
        } else if (edgeIds.containsKey(name)) {
            throw new InvalidIdException("An edge with the same unique name already exists within graph buffer");
        } else {
            ElementId id = ElementId.create();
            edgeIds.put(name, id);
            edges.put(id, Edge.create(id, vertexIds.get(src), vertexIds.get(dst), properties, label, this.graphHead.getId()));
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
