package sh.serene.stellarutils.io.impl.local;

import org.apache.commons.io.FileUtils;
import org.junit.*;
import sh.serene.stellarutils.entities.*;
import sh.serene.stellarutils.graph.impl.local.LocalGraph;
import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;
import sh.serene.stellarutils.testutils.TestGraphUtil;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class LocalIOTest {

    /**
     * temporary directory for reading/writing
     */
    private final String testPath = "tmp/";

    @Before
    public void setUp() throws Exception {
        new File(testPath).mkdirs();
    }

    @Test
    public void testJson() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollectionWrite = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollectionWrite.write().format("json").save(testPath + "tmp.epgm");
        LocalGraphCollection graphCollectionRead = (new LocalReader()).format("json").getGraphCollection(testPath + "tmp.epgm");
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
    public void testWriteNullPath() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollection = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollection.write().format("json").save(null);
    }

    @Test(expected=NullPointerException.class)
    public void testReadNullPath() throws Exception {
        LocalGraphCollection graphCollection = (new LocalReader()).format("json").getGraphCollection(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWriteParquet() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollection = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollection.write().format("parquet").save(testPath + "tmp.parquet");
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testReadParquet() throws Exception {
        LocalGraphCollection graphCollection = (new LocalReader()).format("parquet")
                .getGraphCollection(testPath + "tmp.parquet");
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWriteInvalidFormat() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollection = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollection.write().format("something").save(testPath + "tmp.something");
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testReadInvalidFormat() throws Exception {
        LocalGraphCollection graphCollection = (new LocalReader()).format("something").getGraphCollection(testPath + "tmp.something");
    }

    /**
     * Only tested for success==true
     *
     * @throws Exception
     */
    @Test
    public void testWriteGdf() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollection = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        assertTrue(graphCollection.write().format("gdf").save(testPath + "tmp.gdf"));
    }

    @After
    public void tearDown() {
        try {
            FileUtils.deleteDirectory(new File(testPath));
        } catch (IOException e) {
            System.out.println("Unable to delete temporary folder");
        }
    }

}