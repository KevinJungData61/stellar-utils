package sh.serene.sereneutils.model.epgm;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

public class PropertyGraph extends GraphCollection implements Serializable {

    @Deprecated
    public PropertyGraph() { }

    private PropertyGraph(
            Dataset<GraphHead> graphHeads,
            Dataset<VertexCollection> vertices,
            Dataset<EdgeCollection> edges
    ) {
        super(graphHeads, vertices, edges);
    }

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

    private Dataset<VertexCollection> verticesToCollection() {
        ElementId graphId = this.getGraphId();
        return this.getVertices()
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

    private Dataset<EdgeCollection> edgesToCollection() {
        ElementId graphId = this.getGraphId();
        return this.getEdges()
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

    public GraphCollection toCollection() {
        return new GraphCollection(this.getGraphHeads(), verticesToCollection(), edgesToCollection());
    }

    public GraphCollection intoCollection(GraphCollection graphCollection) {
        if (graphCollection == null) {
            return null;
        }

        Dataset<GraphHead> graphHeads = this.getGraphHeads().union(graphCollection.getGraphHeads());
        Dataset<VertexCollection> vertices = joinVertexCollections(
                verticesToCollection(),
                graphCollection.getVertices()
        );
        Dataset<EdgeCollection> edges = joinEdgeCollections(
                edgesToCollection(),
                graphCollection.getEdges()
        );
        return new GraphCollection(graphHeads, vertices, edges);
    }

    public ElementId getGraphId() {
        return this.getGraphHeads().first().getId();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof PropertyGraph) && ((PropertyGraph) obj).getGraphId().equals(this.getGraphId());
    }

    private Dataset<GraphHead> createGraphHead() {
        return this.getGraphHeads().map(
                (MapFunction<GraphHead,GraphHead>) g -> g.copy(),
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
