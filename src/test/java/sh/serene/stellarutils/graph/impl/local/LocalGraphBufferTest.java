package sh.serene.stellarutils.graph.impl.local;

import org.junit.Test;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.exceptions.InvalidIdException;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphBuffer;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class LocalGraphBufferTest {

    @Test
    public void testEmptyGraph() {
        String label = "empty graph";
        StellarGraphBuffer graphBuffer = new LocalGraphBuffer(label, null);
        StellarGraph graph = graphBuffer.toGraph();
        assertEquals(label, graph.getGraphHead().getLabel());
        assertEquals(0, graph.getGraphHead().getProperties().size());
        assertEquals(0L, graph.getVertices().size());
        assertEquals(0L, graph.getEdges().size());
    }

    @Test
    public void testSimpleGraph() {
        String label = "simple graph";
        StellarGraphBuffer graphBuffer = new LocalGraphBuffer(label, null);
        ElementId vid1 = graphBuffer.addVertex("v1","vertex", null);
        ElementId vid2 = graphBuffer.addVertex("v2","vertex", null);
        ElementId eid3 = graphBuffer.addEdge("e1", "v1", "v2", "edge",null);
        StellarGraph graph = graphBuffer.toGraph();
        assertEquals(label, graph.getGraphHead().getLabel());
        assertEquals(2L, graph.getVertices().size());
        assertEquals(1L, graph.getEdges().size());
        Edge edge = graph.getEdges().asList().get(0);
        assertEquals(eid3, edge.getId());
        assertEquals(vid1, edge.getSrc());
        assertEquals(vid2, edge.getDst());
        assertEquals(graph.getGraphHead().getId(), edge.getVersion());
        HashSet<ElementId> vids = new HashSet<>();
        vids.add(vid1);
        vids.add(vid2);
        List<Vertex> vertices = graph.getVertices().asList();
        assertEquals(vids, vertices.stream().map(Vertex::getId).collect(Collectors.toSet()));
        vertices.stream().map(Vertex::getVersion)
                .forEach(version -> assertEquals(graph.getGraphHead().getId(), version));
    }

    @Test(expected = InvalidIdException.class)
    public void testInvalidEdge() {
        StellarGraphBuffer graphBuffer = new LocalGraphBuffer("invalid", null);
        graphBuffer.addEdge("e", "inv", "alid", "edge", null);
    }

}