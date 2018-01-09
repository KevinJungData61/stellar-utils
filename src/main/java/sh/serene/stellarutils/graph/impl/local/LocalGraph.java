package sh.serene.stellarutils.graph.impl.local;

import scala.Tuple2;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.functions.EdgeMergeFunction;
import sh.serene.stellarutils.functions.PropertiesMergeFunction;
import sh.serene.stellarutils.functions.VertexMergeFunction;
import sh.serene.stellarutils.graph.api.StellarEdgeMemory;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphMemory;
import sh.serene.stellarutils.graph.api.StellarVertexMemory;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalGraph implements StellarGraph {

    /**
     * EPGM Graph Head
     */
    private GraphHead graphHead;

    /**
     * EPGM Vertices
     */
    private List<Vertex> vertices;

    /**
     * EPGM Edges
     */
    private List<Edge> edges;

    @Deprecated
    public LocalGraph() { }

    public LocalGraph(GraphHead graphHead, List<Vertex> vertices, List<Edge> edges) {
        this.graphHead = graphHead;
        this.vertices = new ArrayList<>(vertices);
        this.edges = new ArrayList<>(edges);
    }

    /**
     * Transform vertices to the EPGM Graph Collection format. This is done by appending the graphID list
     * of each vertex with the current Property Graph's ID.
     *
     * @return  vertices
     */
    private List<VertexCollection> verticesToCollection() {
        ElementId graphId = this.graphHead.getId();
        List<VertexCollection> vertexCollections = new ArrayList<>(this.vertices.size());
        for (Vertex v : this.vertices) {
            List<ElementId> graphs = new ArrayList<>();
            graphs.add(v.version());
            if (v.version() != graphId) {
                graphs.add(graphId);
            }
            vertexCollections.add(VertexCollection.create(
                    v.getId(),
                    v.getProperties(),
                    v.getLabel(),
                    graphs
            ));
        }
        return vertexCollections;
    }

    /**
     * Transform edges to the EPGM Graph Collection format. This is done by appending the graphID list
     * of each edge with the current Property Graph's ID.
     *
     * @return edges
     */
    private List<EdgeCollection> edgesToCollection() {
        ElementId graphId = this.graphHead.getId();
        List<EdgeCollection> edgeCollections = new ArrayList<>(this.edges.size());
        for (Edge e : this.edges) {
            List<ElementId> graphs = new ArrayList<>();
            graphs.add(e.version());
            if (e.version() != graphId) {
                graphs.add(graphId);
            }
            edgeCollections.add(EdgeCollection.create(
                    e.getId(),
                    e.getSrc(),
                    e.getDst(),
                    e.getProperties(),
                    e.getLabel(),
                    graphs
            ));
        }
        return edgeCollections;
    }

    /**
     * Turns the Property Graph into a Graph Collection
     *
     * @return  graph collection
     */
    public LocalGraphCollection toCollection() {
        return new LocalGraphCollection(
                Collections.singletonList(this.graphHead), verticesToCollection(), edgesToCollection());
    }

    /**
     * Get graph head
     *
     * @return graph head
     */
    @Override
    public GraphHead getGraphHead() {
        return this.graphHead;
    }

    /**
     * Get vertices
     *
     * @return vertices
     */
    @Override
    public LocalGraphMemory<Vertex> getVertices() {
        return new LocalGraphMemory<>(this.vertices);
    }

    /**
     * Get edges
     *
     * @return edges
     */
    @Override
    public LocalGraphMemory<Edge> getEdges() {
        return new LocalGraphMemory<>(this.edges);
    }

    /**
     * Union two stellar graphs. A new empty graph head is created for the new graph.
     *
     * @param other other graph
     * @return graph containing vertices and edges of both graphs
     */
    @Override
    public LocalGraph union(StellarGraph other) {
        return union(other, (v1, v2) -> v1.getProperties(), (e1, e2) -> e1.getProperties());
    }

    public LocalGraph union(StellarGraph other, PropertiesMergeFunction propsMergeFunc) {
        return union(other, propsMergeFunc, propsMergeFunc);
    }

    public LocalGraph union(
            StellarGraph other,
            PropertiesMergeFunction vertexPropsMergeFunc,
            PropertiesMergeFunction edgePropsMergeFunc
    ) {
        if (other instanceof LocalGraph) {
            ElementId graphId = ElementId.create();
            GraphHead graphHeadNew = GraphHead.create(graphId, null, "");
            List<Vertex> verticesUnioned = new ArrayList<>(
                    Stream.concat(this.vertices.stream(), ((LocalGraph) other).vertices.stream())
                            .collect(Collectors.toMap(
                                    Vertex::getId,
                                    Function.identity(),
                                    new VertexMergeFunction(graphId, vertexPropsMergeFunc)))
                            .values()
            );
            List<Edge> edgesUnioned = new ArrayList<>(
                    Stream.concat(this.edges.stream(), ((LocalGraph) other).edges.stream())
                            .collect(Collectors.toMap(
                                    Edge::getId,
                                    Function.identity(),
                                    new EdgeMergeFunction(graphId, edgePropsMergeFunc)))
                            .values()
            );
            return new LocalGraph(graphHeadNew, verticesUnioned, edgesUnioned);
        } else {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    /**
     * Union a set of vertices into current graph
     *
     * @param vertices vertices
     * @return new graph
     */
    @Override
    public LocalGraph unionVertices(StellarGraphMemory<Vertex> vertices) {
        List<Vertex> verticesNew = new ArrayList<>(this.vertices);
        verticesNew.addAll(vertices.asList());
        return new LocalGraph(
                GraphHead.create(
                        ElementId.create(),
                        new HashMap<>(this.graphHead.getProperties()),
                        this.graphHead.getLabel()
                ),
                verticesNew,
                new ArrayList<>(this.edges)
        );
    }

    /**
     * Union a set of edges into current graph
     *
     * @param edges edges
     * @return new graph
     */
    @Override
    public LocalGraph unionEdges(StellarGraphMemory<Edge> edges) {
        List<Edge> edgesNew = new ArrayList<>(this.edges);
        edgesNew.addAll(edges.asList());
        return new LocalGraph(
                GraphHead.create(
                        ElementId.create(),
                        new HashMap<>(this.graphHead.getProperties()),
                        this.graphHead.getLabel()
                ),
                new ArrayList<>(this.vertices),
                edgesNew
        );
    }

    @Override
    public StellarGraph union(StellarVertexMemory vertexMemory) {
        //TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public StellarGraph union(StellarEdgeMemory edgeMemory) {
        //TODO
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Get edge list
     *
     * @return edge list
     */
    @Override
    public LocalGraphMemory<Tuple2<ElementId, ElementId>> getEdgeList() {
        return new LocalGraphMemory<>(
                this.edges.stream()
                        .map(edge -> new Tuple2<>(edge.getSrc(), edge.getDst()))
                        .collect(Collectors.toList())
        );
    }
}
