package sh.serene.sereneutils.model.graph;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import sh.serene.sereneutils.model.common.ElementId;
import sh.serene.sereneutils.model.common.GraphHead;
import sh.serene.sereneutils.model.common.PropertyValue;
import sh.serene.sereneutils.model.epgm.*;

import java.util.HashMap;
import java.util.Map;

public class OrdinaryGraph {

    private final GraphHead graphHead;

    private final Dataset<Vertex> vertices;

    private final Dataset<Edge> edges;

    private OrdinaryGraph(GraphHead graphHead, Dataset<Vertex> vertices, Dataset<Edge> edges) {
        this.graphHead = graphHead;
        this.vertices = vertices;
        this.edges = edges;
    }

    public static OrdinaryGraph fromGraphCollectionById(EPGMGraphCollection graphCollection, ElementId graphId) {
        Dataset<GraphHead> graphHeads = graphCollection.getGraphHeads()
                .filter((FilterFunction<GraphHead>) graphHead1 -> graphHead1.getId().equals(graphId));
        if (graphHeads.count() != 1) {
            return null;
        }
        Dataset<Vertex> vertices = graphCollection.getVertices()
                .filter((FilterFunction<EPGMVertex>) vertex -> vertex.getGraphs().contains(graphId))
                .map((MapFunction<EPGMVertex,Vertex>) Vertex::fromEPGM, Encoders.bean(Vertex.class));
        Dataset<Edge> edges = graphCollection.getEdges()
                .filter((FilterFunction<EPGMEdge>) edge -> edge.getGraphs().contains(graphId))
                .map((MapFunction<EPGMEdge,Edge>) Edge::fromEPGM, Encoders.bean(Edge.class));
        return new OrdinaryGraph(graphHeads.first(), vertices, edges);
    }

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
        return (obj instanceof OrdinaryGraph) && ((OrdinaryGraph) obj).getGraphId().equals(this.getGraphId());
    }

    public OrdinaryGraph aggregate(String propertyKey, MapFunction<OrdinaryGraph,PropertyValue> func) {
        try {
            PropertyValue propertyValue = func.call(this);
            ElementId graphId = ElementId.create();
            Map<String,PropertyValue> properties = new HashMap<>(this.graphHead.getProperties());
            properties.put(propertyKey, propertyValue);
            GraphHead graphHeadNew = GraphHead.create(graphId, properties, this.graphHead.getLabel());
            return new OrdinaryGraph(graphHeadNew, this.vertices, this.edges);
        } catch (Exception e) {
            return null;
        }
    }

    // TODO
    // these are all wrong (?)

    public OrdinaryGraph except(OrdinaryGraph g) {
        Dataset<Vertex> verticesNew = this.vertices.except(g.getVertices());
        Dataset<Edge> edgesNew = this.edges.except(g.getEdges());
        GraphHead graphHeadNew = GraphHead.create(ElementId.create(), this.graphHead.getProperties(), this.graphHead.getLabel());
        return new OrdinaryGraph(graphHeadNew, verticesNew, edgesNew);
    }

    public OrdinaryGraph intersect(OrdinaryGraph g) {
        Dataset<Vertex> verticesNew = this.vertices.intersect(g.getVertices());
        Dataset<Edge> edgesNew = this.edges.intersect(g.getEdges());
        GraphHead graphHeadNew = GraphHead.create(ElementId.create(), this.graphHead.getProperties(), this.graphHead.getLabel());
        return new OrdinaryGraph(graphHeadNew, verticesNew, edgesNew);
    }

    public OrdinaryGraph union(OrdinaryGraph g) {
        Dataset<Vertex> verticesNew = this.vertices.union(g.getVertices());
        Dataset<Edge> edgesNew = this.edges.union(g.getEdges());
        GraphHead graphHeadNew = GraphHead.create(ElementId.create(), this.graphHead.getProperties(), this.graphHead.getLabel());
        return new OrdinaryGraph(graphHeadNew, verticesNew, edgesNew);
    }

}
