package sh.serene.stellarutils.model.epgm;

import org.apache.spark.api.java.function.FilterFunction;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class GraphCollectionBuilderTest implements Serializable {

    private GraphCollectionBuilder builder;
    private ElementId graph;
    private ElementId vertex1;
    private ElementId vertex2;
    private ElementId edge;
    private Properties properties;

    @Before
    public void setUp() throws Exception {
        builder = new GraphCollectionBuilder();
        properties = Properties.create();
        properties.add("string", "value");
        properties.add("boolean", true);
        properties.add("integer", 123);
        graph = builder.addGraphHead(properties.getMap(), "graph");
        List<ElementId> graphs = Collections.singletonList(graph);
        vertex1 = builder.addVertex(properties.getMap(), "vertex", graphs);
        vertex2 = builder.addVertex(properties.getMap(), "vertex2", graphs);
        edge = builder.addEdge(vertex1, vertex2, properties.getMap(), "edge", graphs);
    }

    private <T extends Element> void assertEqualsProperty(String propertyKey, T element) throws Exception {
        assertEquals(properties.getMap().get(propertyKey), element.getProperty(propertyKey));
    }

    @Test
    public void testGraphHead() throws Exception {
        GraphCollection graphCollection = builder.toGraphCollection();
        assertEquals(1, graphCollection.getGraphHeads().count());
        GraphHead graphHead = graphCollection.getGraphHeads().first();
        assertEquals(graph, graphHead.getId());
        assertEqualsProperty("string", graphHead);
        assertEqualsProperty("boolean", graphHead);
        assertEqualsProperty("integer", graphHead);
    }

    @Test
    public void testVertex() throws Exception {
        GraphCollection graphCollection = builder.toGraphCollection();
        assertEquals(2, graphCollection.getVertices().count());
        assertEquals(
                1,
                graphCollection
                        .getVertices()
                        .filter((FilterFunction<VertexCollection>) v -> v.getId().equals(vertex1))
                        .count()
        );
        assertEquals(
                1,
                graphCollection
                        .getVertices()
                        .filter((FilterFunction<VertexCollection>) v -> v.getId().equals(vertex2))
                        .count()
        );
        VertexCollection vertexCollection = graphCollection.getVertices().first();
        assertEqualsProperty("string", vertexCollection);
        assertEqualsProperty("boolean", vertexCollection);
        assertEqualsProperty("integer", vertexCollection);
    }

    @Test
    public void testEdge() throws Exception {
        GraphCollection graphCollection = builder.toGraphCollection();
        assertEquals(1, graphCollection.getEdges().count());
        EdgeCollection edgeCollection = graphCollection.getEdges().first();
        assertEquals(edge, edgeCollection.getId());
        assertEquals(vertex1, edgeCollection.getSrc());
        assertEquals(vertex2, edgeCollection.getDst());
        assertEqualsProperty("string", edgeCollection);
        assertEqualsProperty("boolean", edgeCollection);
        assertEqualsProperty("integer", edgeCollection);
    }

}