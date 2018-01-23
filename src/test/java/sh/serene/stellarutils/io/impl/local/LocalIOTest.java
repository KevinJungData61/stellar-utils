package sh.serene.stellarutils.io.impl.local;

import org.json.simple.JSONObject;
import org.junit.Test;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.impl.local.LocalGraph;
import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;
import sh.serene.stellarutils.testutils.TestGraphUtil;

import java.util.List;

import static org.junit.Assert.*;

public class LocalIOTest {
    @Test
    public void testJson() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollectionWrite = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollectionWrite.write().format("json").save("./tmp.epgm");
        LocalGraphCollection graphCollectionRead = (new LocalReader()).format("json").getGraphCollection("./tmp.epgm");
        for (ElementId graphId : util.getGraphIdList()) {
            LocalGraph g = graphCollectionRead.get(graphId);
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

    @Test(expected=NullPointerException.class)
    public void testNullPath() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollection = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollection.write().format("json").save(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWriteParquet() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollection = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollection.write().format("parquet").save(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWriteInvalidFormat() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollection = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollection.write().format("something").save(null);
    }

    /**
     * Only tested for no-fail
     *
     * @throws Exception
     */
    @Test
    public void testWriteGdf() throws Exception {

    }

}