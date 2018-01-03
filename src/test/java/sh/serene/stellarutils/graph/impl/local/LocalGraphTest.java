package sh.serene.stellarutils.graph.impl.local;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.testutils.TestGraphUtil;

import static org.junit.Assert.*;

public class LocalGraphTest {

    private LocalGraphCollection gc;
    private TestGraphUtil util;

    @Before
    public void setUp() {
        util = TestGraphUtil.getInstance();
        gc = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList());
    }

    @Test
    public void write() throws Exception {
        assertTrue(gc.write() != null);
    }

    @Test
    public void TestCollectionGet() throws Exception {
        for (ElementId graphId : util.getGraphIdList()) {
            LocalGraph g = gc.get(graphId);
            assertEquals(g.getVertices().asList().size(), util.getVertexCount(graphId));
            assertEquals(g.getEdges().asList().size(), util.getEdgeCount(graphId));
            GraphHead gh = g.getGraphHead();
            assertEquals(gh.getProperties(), util.getGraphProperties(graphId));
            assertEquals(gh.getLabel(), util.getGraphLabel(graphId));
            for (Vertex v : g.getVertices().asList()) {
                assertEquals(v.getProperties(), util.getVertexProperties(v.getId()));
                assertEquals(v.getLabel(), util.getVertexLabel(v.getId()));
                assertEquals(v.getVersion(), util.getVertexVersion(v.getId()));
            }
            for (Edge e : g.getEdges().asList()) {
                assertEquals(e.getSrc(), util.getEdgeSrc(e.getId()));
                assertEquals(e.getDst(), util.getEdgeDst(e.getId()));
                assertEquals(e.getProperties(), util.getEdgeProperties(e.getId()));
                assertEquals(e.getLabel(), util.getEdgeLabel(e.getId()));
                assertEquals(e.getVersion(), util.getEdgeVersion(e.getId()));
            }
        }
    }

    @Test
    public void TestCollectionUnion() throws Exception {
    }

    @Test
    public void union1() throws Exception {
    }

}