package sh.serene.sereneutils.model.epgm;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class OrdinaryGraph extends GraphCollection {

    private OrdinaryGraph(Dataset<GraphHead> graphHead, Dataset<Vertex> vertices, Dataset<Edge> edges) {
        super(graphHead, vertices, edges);
    }

    public OrdinaryGraph fromGraphCollectionById(GraphCollection graphCollection, ElementId graphId) {
        Dataset<GraphHead> graphHead = graphCollection.getGraphHeads()
                .filter((FilterFunction<GraphHead>) graphHead1 -> graphHead1.getId().equals(graphId));
        if (graphHead.count() != 1) {
            return null;
        }
        Dataset<Vertex> vertices = graphCollection.getVertices()
                .filter((FilterFunction<Vertex>) vertex -> vertex.getGraphs().contains(graphId));
        Dataset<Edge> edges = graphCollection.getEdges()
                .filter((FilterFunction<Edge>) edge -> edge.getGraphs().contains(graphId));
        return new OrdinaryGraph(graphHead, vertices, edges);
    }

    public OrdinaryGraph fromGraphCollection(GraphCollection graphCollection) {
        if (graphCollection.getGraphHeads().count() != 1) {
            return null;
        }
        return new OrdinaryGraph(graphCollection.getGraphHeads(), graphCollection.getVertices(), graphCollection.getEdges());
    }

    public OrdinaryGraph aggregate(String propertyKey, MapFunction<OrdinaryGraph,PropertyValue> func) {
        try {
            PropertyValue propertyValue = func.call(this);

            // TODO
            // strictly speaking, all element IDs should be created new again, when any sort of operation is done
            // in order to ensure everything is immutable. A simple aggregation operation leading to an extra property
            // being added to the graph head is gonna lead to rewriting all the vertices and edges.
            // Furthermore, these new elements can't be compared back to the original vertices and edges at all
            // since all their IDs have changed
            ElementId graphId = ElementId.create();
            Dataset<GraphHead> graphHead = this.graphHeads.map((MapFunction<GraphHead,GraphHead>) g -> {
                Map<String,PropertyValue> properties = new HashMap<>(g.properties);
                properties.put(propertyKey, propertyValue);
                return GraphHead.create(graphId, properties, g.label);
            }, Encoders.bean(GraphHead.class));
            Dataset<Vertex> vertices = this.vertices.map((MapFunction<Vertex,Vertex>) vertex -> {
                // TODO
                return vertex;
            }, Encoders.bean(Vertex.class));
            Dataset<Edge> edges = this.edges.map((MapFunction<Edge,Edge>) edge -> {
                // TODO
                return edge;
            },Encoders.bean(Edge.class));
            return new OrdinaryGraph(graphHead, vertices, edges);
        } catch (Exception e) {
            return null;
        }
    }
}
