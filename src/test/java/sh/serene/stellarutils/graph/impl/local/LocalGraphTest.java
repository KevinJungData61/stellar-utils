package sh.serene.stellarutils.graph.impl.local;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.testutils.TestGraphUtil;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static sh.serene.stellarutils.testutils.TestGraphUtil.*;

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
    public void testCollectionGet() throws Exception {
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
    public void testCollectionUnion() throws Exception {
        LocalGraph gEmpty = gc.get(graphIds.get(G_EMPTY));
        LocalGraph gBaseline = gc.get(graphIds.get(G_BASE_LINE));
        LocalGraph gNewInfo = gc.get(graphIds.get(G_NEW_INFO));
        LocalGraph gPreEr = gc.get(graphIds.get(G_PRE_ER));
        LocalGraph gPostEr = gc.get(graphIds.get(G_POST_ER));
        LocalGraphCollection gcEmptyAndBaseline = gEmpty.toCollection().union(gBaseline);
        LocalGraphCollection gcNewInfoAndPreEr = gNewInfo.toCollection().union(gPreEr);
        LocalGraphCollection gcAll = gcEmptyAndBaseline.union(gcNewInfoAndPreEr).union(gPostEr);
        assertEquals(gcAll.vertices.size(), util.getVertexCount(graphIds.get(G_POST_ER)));
        assertEquals(gcAll.edges.size(), util.getEdgeCount(graphIds.get(G_POST_ER)));
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
    public void testUnionVertices() throws Exception {
        LocalGraph gEmpty = gc.get(graphIds.get(G_EMPTY));
        LocalGraph gBaseline = gc.get(graphIds.get(G_BASE_LINE));
        LocalGraph gNewInfo = gc.get(graphIds.get(G_NEW_INFO));

        LocalGraph gEmpty2 = gEmpty.unionVertices(new LocalGraphMemory<>(new ArrayList<>()));
        assertEquals(0L, gEmpty2.getVertices().size());

        LocalGraph gBaseline2 = gEmpty2.unionVertices(gBaseline.getVertices());
        assertEquals(util.getVertexCount(graphIds.get(G_BASE_LINE)), gBaseline2.getVertices().asList().size());
        for (Vertex v : gBaseline2.getVertices().asList()) {
            assertEquals(util.getVertexProperties(v.getId()), v.getProperties());
            assertEquals(util.getVertexLabel(v.getId()), v.getLabel());
            assertEquals(util.getVertexVersion(v.getId()), v.getVersion());
        }

        LocalGraph gPreEr2 = gBaseline2.unionVertices(gNewInfo.getVertices());
        assertEquals(util.getVertexCount(graphIds.get(G_PRE_ER)), gPreEr2.getVertices().asList().size());
        for (Vertex v : gPreEr2.getVertices().asList()) {
            assertEquals(util.getVertexProperties(v.getId()), v.getProperties());
            assertEquals(util.getVertexLabel(v.getId()), v.getLabel());
            assertEquals(util.getVertexVersion(v.getId()), v.getVersion());
        }
    }

    @Test
    public void testUnionEdges() throws Exception {
        LocalGraph gEmpty = gc.get(graphIds.get(G_EMPTY));
        LocalGraph gBaseline = gc.get(graphIds.get(G_BASE_LINE));
        LocalGraph gNewInfo = gc.get(graphIds.get(G_NEW_INFO));

        LocalGraph gEmpty2 = gEmpty.unionEdges(new LocalGraphMemory<>(new ArrayList<>()));
        assertEquals(0L, gEmpty2.getEdges().size());

        LocalGraph gBaseline2 = gEmpty2.unionEdges(gBaseline.getEdges());
        assertEquals(util.getEdgeCount(graphIds.get(G_BASE_LINE)), gBaseline2.getEdges().asList().size());
        for (Edge e : gBaseline2.getEdges().asList()) {
            assertEquals(util.getEdgeSrc(e.getId()), e.getSrc());
            assertEquals(util.getEdgeDst(e.getId()), e.getDst());
            assertEquals(util.getEdgeProperties(e.getId()), e.getProperties());
            assertEquals(util.getEdgeLabel(e.getId()), e.getLabel());
            assertEquals(util.getEdgeVersion(e.getId()), e.getVersion());
        }

        LocalGraph gPreEr2 = gBaseline2.unionEdges(gNewInfo.getEdges());
        assertEquals(util.getEdgeCount(graphIds.get(G_PRE_ER)), gPreEr2.getEdges().asList().size());
        for (Edge e : gPreEr2.getEdges().asList()) {
            assertEquals(util.getEdgeSrc(e.getId()), e.getSrc());
            assertEquals(util.getEdgeDst(e.getId()), e.getDst());
            assertEquals(util.getEdgeProperties(e.getId()), e.getProperties());
            assertEquals(util.getEdgeLabel(e.getId()), e.getLabel());
            assertEquals(util.getEdgeVersion(e.getId()), e.getVersion());
        }
    }

    @Test
    public void testGetEdgeList() throws Exception {
        Map<ElementId,List<ElementId>> edgelist = new HashMap<>();
        LocalGraph g = gc.get(graphIds.get(G_POST_ER));
        g.getEdgeList().asList().forEach(tup ->
            edgelist.merge(tup._1, Collections.singletonList(tup._2), (l1, l2) -> {
                List<ElementId> l3 = new ArrayList<>(l1);
                l3.addAll(l2);
                return l3;
            })
        );
        assertEquals(g.getEdges().size(), g.getEdgeList().size());
        for (ElementId id : g.getEdges().asList().stream().map(Edge::getId).collect(Collectors.toList())) {
            ElementId src = util.getEdgeSrc(id);
            ElementId dst = util.getEdgeDst(id);
            assertTrue(edgelist.get(src).contains(dst));
        }
    }

}