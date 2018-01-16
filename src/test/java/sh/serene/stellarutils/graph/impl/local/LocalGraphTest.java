package sh.serene.stellarutils.graph.impl.local;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.api.StellarGraph;
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
            g.getVertices().asList().forEach(this::testVertex);
            g.getEdges().asList().forEach(this::testEdge);
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
        gBaseline2.getVertices().asList().forEach(this::testVertex);

        LocalGraph gPreEr2 = gBaseline2.unionVertices(gNewInfo.getVertices());
        assertEquals(util.getVertexCount(graphIds.get(G_PRE_ER)), gPreEr2.getVertices().asList().size());
        gPreEr2.getVertices().asList().forEach(this::testVertex);
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
        gBaseline2.getEdges().asList().forEach(this::testEdge);

        LocalGraph gPreEr2 = gBaseline2.unionEdges(gNewInfo.getEdges());
        assertEquals(util.getEdgeCount(graphIds.get(G_PRE_ER)), gPreEr2.getEdges().asList().size());
        gPreEr2.getEdges().asList().forEach(this::testEdge);
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

    @Test
    public void testUnion() throws Exception {
        LocalGraph gEmpty = gc.get(graphIds.get(G_EMPTY));
        LocalGraph gBaseline = gc.get(graphIds.get(G_BASE_LINE));
        LocalGraph gNewInfo = gc.get(graphIds.get(G_NEW_INFO));

        LocalGraph gBaseline2 = gEmpty.union(gBaseline);
        assertEquals(util.getVertexCount(graphIds.get(G_BASE_LINE)), gBaseline2.getVertices().asList().size());
        assertEquals(util.getEdgeCount(graphIds.get(G_BASE_LINE)), gBaseline2.getEdges().asList().size());
        gBaseline2.getVertices().asList().forEach(this::testVertex);
        gBaseline2.getEdges().asList().forEach(this::testEdge);

        LocalGraph gPreEr = gBaseline.union(gNewInfo);
        assertEquals(util.getVertexCount(graphIds.get(G_PRE_ER)), gPreEr.getVertices().asList().size());
        assertEquals(util.getEdgeCount(graphIds.get(G_PRE_ER)), gPreEr.getEdges().asList().size());
        gPreEr.getVertices().asList().forEach(this::testVertex);
        gPreEr.getEdges().asList().forEach(this::testEdge);

        LocalGraph gPreEr2 = gBaseline.union(gPreEr);
        assertEquals(util.getVertexCount(graphIds.get(G_PRE_ER)), gPreEr2.getVertices().asList().size());
        assertEquals(util.getEdgeCount(graphIds.get(G_PRE_ER)), gPreEr2.getEdges().asList().size());
        gPreEr2.getVertices().asList().forEach(this::testVertex);
        gPreEr2.getEdges().asList().forEach(this::testEdge);

    }

    @Test
    public void testConnectedComponents() throws Exception {
        LocalGraph gPreEr = gc.get(graphIds.get(G_PRE_ER));
        List<StellarGraph> graphs = gPreEr.getConnectedComponents();
        int n_baseline = 0;
        int n_newinfo = 0;
        assertEquals(2, graphs.size());
        for (StellarGraph g : graphs) {
            if (g.getVertices().asList().size() == util.getVertexCount(graphIds.get(G_BASE_LINE))) {
                // BASELINE
                assertEquals(util.getEdgeCount(graphIds.get(G_BASE_LINE)), g.getEdges().asList().size());
                n_baseline++;
            } else if (g.getVertices().asList().size() == util.getVertexCount(graphIds.get(G_NEW_INFO))) {
                // NEW INFO
                assertEquals(util.getEdgeCount(graphIds.get(G_NEW_INFO)), g.getEdges().asList().size());
                n_newinfo++;
            } else {
                fail();
            }
            g.getVertices().asList().forEach(this::testVertex);
            g.getEdges().asList().forEach(this::testEdge);
        }
        assertTrue((n_baseline == 1) && (n_newinfo == 1));

        LocalGraph gPostEr = gc.get(graphIds.get(G_POST_ER));
        List<StellarGraph> graphsPost = gPostEr.getConnectedComponents();
        assertEquals(1, graphsPost.size());
        for (StellarGraph g : graphsPost) {
            assertEquals(util.getVertexCount(graphIds.get(G_POST_ER)), g.getVertices().asList().size());
            assertEquals(util.getEdgeCount(graphIds.get(G_POST_ER)), g.getEdges().asList().size());
            g.getVertices().asList().forEach(this::testVertex);
            g.getEdges().asList().forEach(this::testEdge);
        }
    }

    @Test
    public void testAdjacencyTuples() throws Exception {
        LocalGraph gPreEr = gc.get(graphIds.get(G_PRE_ER));
        List<AdjacencyTuple> people = gPreEr.getAdjacencyTuples(v -> v.getLabel().equals("Person"));
        for (AdjacencyTuple tuple : people) {
            System.out.println(tuple.source.getPropertyValue("name", String.class));
            tuple.inbound.forEach((e, v) ->
                    System.out.printf(
                            "<-- [%s] -- %s%n",
                            e.getLabel(),
                            v.getProperties().getOrDefault("name", PropertyValue.create("unknown")))
            );
            tuple.outbound.forEach((e, v) ->
                    System.out.printf(
                            "-- [%s] --> %s%n",
                            e.getLabel(),
                            v.getProperties().getOrDefault("name", PropertyValue.create("unknown")))
            );
        }
    }

    private void testVertex(Vertex v) {
        assertEquals(util.getVertexLabel(v.getId()), v.getLabel());
        assertEquals(util.getVertexProperties(v.getId()), v.getProperties());
        assertEquals(util.getVertexVersion(v.getId()), v.getVersion());
    }

    private void testEdge(Edge e) {
        assertEquals(util.getEdgeSrc(e.getId()), e.getSrc());
        assertEquals(util.getEdgeDst(e.getId()), e.getDst());
        assertEquals(util.getEdgeLabel(e.getId()), e.getLabel());
        assertEquals(util.getEdgeProperties(e.getId()), e.getProperties());
        assertEquals(util.getEdgeVersion(e.getId()), e.getVersion());
    }

}