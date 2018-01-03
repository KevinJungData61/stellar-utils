package sh.serene.stellarutils.graph.impl.local;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.testutils.TestGraphUtil;

import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.*;

public class LocalGraphTest {

    private LocalGraphCollection gc;
    private Map<String,ElementId> graphIds;
    private TestGraphUtil util;

    @Before
    public void setUp() {
        util = TestGraphUtil.getInstance();
        gc = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList());
        graphIds = util.getGraphIdMap();
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
        LocalGraph gEmpty = gc.get(graphIds.get(util.G_EMPTY));
        LocalGraph gBaseline = gc.get(graphIds.get(util.G_BASE_LINE));
        LocalGraph gNewInfo = gc.get(graphIds.get(util.G_NEW_INFO));
        LocalGraph gPreEr = gc.get(graphIds.get(util.G_PRE_ER));
        LocalGraph gPostEr = gc.get(graphIds.get(util.G_POST_ER));
        LocalGraphCollection gcEmptyAndBaseline = gEmpty.toCollection().union(gBaseline);
        LocalGraphCollection gcNewInfoAndPreEr = gNewInfo.toCollection().union(gPreEr);
        LocalGraphCollection gcAll = gcEmptyAndBaseline.union(gcNewInfoAndPreEr).union(gPostEr);
        assertEquals(gcAll.vertices.size(), util.getVertexCount(graphIds.get(util.G_POST_ER)));
        assertEquals(gcAll.edges.size(), util.getEdgeCount(graphIds.get(util.G_POST_ER)));
        for (VertexCollection v : gcAll.vertices) {
            assertEquals(v.getProperties(), util.getVertexProperties(v.getId()));
            assertEquals(v.getLabel(), util.getVertexLabel(v.getId()));
            assertEquals(v.version(), util.getVertexVersion(v.getId()));
            assertEquals(v.getGraphs().size(), util.getVertexGraphs(v.getId()).size());
            assertEquals(new HashSet<>(v.getGraphs()), new HashSet<>(util.getVertexGraphs(v.getId())));
        }
        for (EdgeCollection e : gcAll.edges) {
            assertEquals(e.getSrc(), util.getEdgeSrc(e.getId()));
            assertEquals(e.getDst(), util.getEdgeDst(e.getId()));
            assertEquals(e.getProperties(), util.getEdgeProperties(e.getId()));
            assertEquals(e.getLabel(), util.getEdgeLabel(e.getId()));
            assertEquals(e.version(), util.getEdgeVersion(e.getId()));
            assertEquals(e.getGraphs().size(), util.getEdgeGraphs(e.getId()).size());
            assertEquals(new HashSet<>(e.getGraphs()), new HashSet<>(util.getEdgeGraphs(e.getId())));
        }
    }

    @Test
    public void TestUnion() throws Exception {
    }

    @Test
    public void TestUnionVertices() throws Exception {

    }

    @Test
    public void TestUnionEdges() throws Exception {

    }

    @Test
    public void TestGetEdgeList() throws Exception {

    }

}