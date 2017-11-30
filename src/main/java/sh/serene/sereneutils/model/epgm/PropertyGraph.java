package sh.serene.sereneutils.model.epgm;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class PropertyGraph {

    private final GraphHead graphHead;

    private final Dataset<Vertex> vertices;

    private final Dataset<Edge> edges;

    private PropertyGraph(GraphHead graphHead, Dataset<Vertex> vertices, Dataset<Edge> edges) {
        this.graphHead = graphHead;
        this.vertices = vertices;
        this.edges = edges;
    }

    public static PropertyGraph fromEPGM(GraphCollection graphCollection, ElementId graphId) {
        if (graphCollection == null || graphId == null) {
            return null;
        }
        Dataset<GraphHead> graphHeads = graphCollection.getGraphHeads()
                .filter((FilterFunction<GraphHead>) graphHead1 -> graphHead1.getId().equals(graphId));
        if (graphHeads.count() != 1) {
            return null;
        }
        Dataset<Vertex> vertices = graphCollection.getVertices()
                .filter((FilterFunction<VertexCollection>) vertex -> vertex.getGraphs().contains(graphId))
                .map((MapFunction<VertexCollection,Vertex>) Vertex::fromEPGM, Encoders.bean(Vertex.class));
        Dataset<Edge> edges = graphCollection.getEdges()
                .filter((FilterFunction<EdgeCollection>) edge -> edge.getGraphs().contains(graphId))
                .map((MapFunction<EdgeCollection,Edge>) Edge::fromEPGM, Encoders.bean(Edge.class));
        return new PropertyGraph(graphHeads.first(), vertices, edges);
    }

    /*
    public GraphCollection toEPGM(GraphCollection graphCollection) {
        if (graphCollection == null) {
            return null;
        }
        // check if graphid exists
        // for vertices and edges:
        // joinWith outer on id
        // groupby?
        //
    }*/

    public ElementId getGraphId() {
        return this.graphHead.getId();
    }

    public GraphHead getGraphHead() {
        return this.graphHead;
    }

    public Dataset<Vertex> getVertices() {
        return this.vertices;
    }

    public Dataset<Edge> getEdges() {
        return this.edges;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof PropertyGraph) && ((PropertyGraph) obj).getGraphId().equals(this.getGraphId());
    }

    /**
     * Aggregate operator
     *
     * @param propertyKey       key for new graph property
     * @param func              map function to obtain property value from graph
     * @return                  new graph
     */
    public PropertyGraph aggregate(String propertyKey, MapFunction<PropertyGraph,PropertyValue> func) {
        try {
            PropertyValue propertyValue = func.call(this);
            ElementId graphId = ElementId.create();
            Map<String,PropertyValue> properties = new HashMap<>(this.graphHead.getProperties());
            properties.put(propertyKey, propertyValue);
            GraphHead graphHeadNew = GraphHead.create(graphId, properties, this.graphHead.getLabel());
            return new PropertyGraph(graphHeadNew, this.vertices, this.edges);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Add edges to graph. No check is performed to ensure the edges are valid for the current graph, so
     * it is up to the user to make sure this is the case
     *
     * @param edges     new edges to add
     * @return          new graph
     */
    public PropertyGraph addEdges(Dataset<Edge> edges) {
        return new PropertyGraph(this.graphHead.copy(), this.vertices, this.edges.union(edges));
    }

    /**
     * Add vertices to graph. No check is performed to ensure the vertices are valid for the current graph,
     * so it is up to the user to make sure this is the case
     *
     * @param vertices      new vertices to add
     * @return              new graph
     */
    public PropertyGraph addVertices(Dataset<Vertex> vertices) {
        return new PropertyGraph(this.graphHead.copy(), this.vertices.union(vertices), this.edges);
    }

    /**
     * Get edge list as a dataset of tuples (src,dst) from graph.
     *
     * @return  edge list
     */
    public Dataset<Tuple2<ElementId,ElementId>> getEdgeList() {
        return this.edges.map((MapFunction<Edge,Tuple2<ElementId,ElementId>>) edge -> (
                new Tuple2<>(edge.getSrc(), edge.getDst())
                ), Encoders.tuple(Encoders.bean(ElementId.class), Encoders.bean(ElementId.class)));
    }

}
