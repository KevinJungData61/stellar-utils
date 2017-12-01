package sh.serene.sereneutils.model.epgm;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Property Graph - a single graph extracted from an EPGM Graph Collection
 *
 */
public class PropertyGraph implements Serializable {

    /**
     * EPGM Graph Head
     */
    private Dataset<GraphHead> graphHeads;

    /**
     * EPGM Vertices
     */
    private Dataset<VertexCollection> vertices;

    /**
     * EPGM Edges
     */
    private Dataset<EdgeCollection> edges;

    @Deprecated
    public PropertyGraph() { }

    private PropertyGraph(
            Dataset<GraphHead> graphHeads,
            Dataset<VertexCollection> vertices,
            Dataset<EdgeCollection> edges
    ) {
        this.graphHeads = graphHeads;
        this.vertices = vertices;
        this.edges = edges;
    }

    /**
     * Get Property Graph from collection by ID
     *
     * @param graphCollection   graph collection
     * @param graphId           ID of property graph
     * @return                  property graph
     */
    public static PropertyGraph fromCollection(GraphCollection graphCollection, ElementId graphId) {

        // check null
        if (graphCollection == null || graphId == null) {
            return null;
        }

        // check graph head count
        Dataset<GraphHead> graphHeads = graphCollection.getGraphHeads()
                .filter((FilterFunction<GraphHead>) graphHead1 -> graphHead1.getId().equals(graphId));
        if (graphHeads.count() != 1) {
            return null;
        }

        // get vertices and edges from collection
        Dataset<VertexCollection> vertices = graphCollection.getVertices()
                .filter((FilterFunction<VertexCollection>) vertex -> vertex.getGraphs().contains(graphId))
                .map((MapFunction<VertexCollection,VertexCollection>) vertex -> VertexCollection.create(
                        vertex.getId(),
                        vertex.getProperties(),
                        vertex.getLabel(),
                        Collections.singletonList(vertex.getGraphs().get(0))
                ), Encoders.bean(VertexCollection.class));
        Dataset<EdgeCollection> edges = graphCollection.getEdges()
                .filter((FilterFunction<EdgeCollection>) edge -> edge.getGraphs().contains(graphId))
                .map((MapFunction<EdgeCollection,EdgeCollection>) edge -> EdgeCollection.create(
                        edge.getId(),
                        edge.getSrc(),
                        edge.getDst(),
                        edge.getProperties(),
                        edge.getLabel(),
                        Collections.singletonList(edge.getGraphs().get(0))
                ), Encoders.bean(EdgeCollection.class));

        return new PropertyGraph(graphHeads, vertices, edges);
    }

    /**
     * Transform vertices to the EPGM Graph Collection format. This is done by appending the graphID list
     * of each vertex with the current Property Graph's ID.
     *
     * @return  vertices
     */
    private Dataset<VertexCollection> verticesToCollection() {
        ElementId graphId = this.getGraphId();
        return this.vertices
                .map((MapFunction<VertexCollection,VertexCollection>) vertex -> {
                    List<ElementId> graphs = new ArrayList<>(vertex.getGraphs());
                    if (graphs.isEmpty() || graphs.get(graphs.size() - 1) != graphId) {
                        graphs.add(graphId);
                    }
                    return VertexCollection.create(
                            vertex.getId(),
                            vertex.getProperties(),
                            vertex.getLabel(),
                            graphs
                    );
                }, Encoders.bean(VertexCollection.class));
    }

    /**
     * Transform edges to the EPGM Graph Collection format. This is done by appending the graphID list of
     * each edge with the current Property Graph's ID.
     *
     * @return  edges
     */
    private Dataset<EdgeCollection> edgesToCollection() {
        ElementId graphId = this.getGraphId();
        return this.edges
                .map((MapFunction<EdgeCollection,EdgeCollection>) edge -> {
                    List<ElementId> graphs = new ArrayList<>(edge.getGraphs());
                    if (graphs.isEmpty() || graphs.get(graphs.size() - 1) != graphId) {
                        graphs.add(graphId);
                    }
                    return EdgeCollection.create(
                            edge.getId(),
                            edge.getSrc(),
                            edge.getDst(),
                            edge.getProperties(),
                            edge.getLabel(),
                            graphs
                    );
                }, Encoders.bean(EdgeCollection.class));
    }

    /**
     * Turns the Property Graph into a Graph Collection
     *
     * @return  graph collection
     */
    public GraphCollection toCollection() {
        return new GraphCollection(this.graphHeads, verticesToCollection(), edgesToCollection());
    }

    /**
     * Writes the Property Graph into a given Graph Collection
     *
     * @param graphCollection   graph collection
     * @return                  new graph collection
     */
    public GraphCollection intoCollection(GraphCollection graphCollection) {
        if (graphCollection == null) {
            return null;
        }

        Dataset<GraphHead> graphHeads = graphCollection.getGraphHeads().union(this.graphHeads);
        Dataset<VertexCollection> vertices = graphCollection.joinVertexCollections(verticesToCollection());
        Dataset<EdgeCollection> edges = graphCollection.joinEdgeCollections(edgesToCollection());

        return new GraphCollection(graphHeads, vertices, edges);
    }

    /**
     * Get Graph ID
     *
     * @return  graph ID
     */
    public ElementId getGraphId() {
        return this.graphHeads.first().getId();
    }

    /**
     * Get graph heads dataset
     *
     * @return  graph heads
     */
    public Dataset<GraphHead> getGraphHeads() {
        return this.graphHeads;
    }

    /**
     * Get vertex dataset
     *
     * @return  vertices
     */
    public Dataset<VertexCollection> getVertices() {
        return this.vertices;
    }

    /**
     * Get edge dataset
     *
     * @return  edges
     */
    public Dataset<EdgeCollection> getEdges() {
        return this.edges;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof PropertyGraph) && ((PropertyGraph) obj).getGraphId().equals(this.getGraphId());
    }

    /**
     * Create a new graph head with the same properties as the current graph head. A new ID is generated.
     *
     * @return  graph head
     */
    private Dataset<GraphHead> createGraphHead() {
        return this.getGraphHeads().map(
                (MapFunction<GraphHead,GraphHead>) GraphHead::copy,
                Encoders.bean(GraphHead.class)
        );
    }

    /**
     * Add edges to graph. No check is performed to ensure the edges are valid for the current graph, so
     * it is up to the user to make sure this is the case
     *
     * @param edges     new edges to add
     * @return          new graph
     */
    public PropertyGraph addEdges(Dataset<EdgeCollection> edges) {
        return new PropertyGraph(createGraphHead(), this.getVertices(), this.getEdges().union(edges));
    }

    /**
     * Add vertices to graph. No check is performed to ensure the vertices are valid for the current graph,
     * so it is up to the user to make sure this is the case
     *
     * @param vertices      new vertices to add
     * @return              new graph
     */
    public PropertyGraph addVertices(Dataset<VertexCollection> vertices) {
        return new PropertyGraph(createGraphHead(), this.getVertices().union(vertices), this.getEdges());
    }

    /**
     * Get edge list as a dataset of tuples (src,dst) from graph.
     *
     * @return  edge list
     */
    public Dataset<Tuple2<ElementId,ElementId>> getEdgeList() {
        return this.getEdges().map((MapFunction<EdgeCollection,Tuple2<ElementId,ElementId>>) edge -> (
                new Tuple2<>(edge.getSrc(), edge.getDst())
                ), Encoders.tuple(Encoders.bean(ElementId.class), Encoders.bean(ElementId.class)));
    }

}
