package sh.serene.sereneutils.io.json;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sh.serene.sereneutils.io.testutils.GraphCollectionFactory;
import sh.serene.sereneutils.io.testutils.GraphCompare;
import sh.serene.sereneutils.model.epgm.*;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * JSON Data Sink & Data Source Test
 *
 */
public class JSONDataTest {

    /**
     * temporary directory for reading/writing
     */
    private final String testPath = "tmp.epgm/";

    private SparkSession spark;
    private JSONDataSink jsonDataSink;
    private JSONDataSource jsonDataSource;

    @Before
    public void setUp() {
        spark = SparkSession.builder().appName("JSON Data Test").master("local").getOrCreate();
        jsonDataSink = new JSONDataSink(testPath);
        jsonDataSource = new JSONDataSource(testPath, spark);
    }

    @Test
    public void testSingleGraphNoAttrNoLabel() {
        GraphCollection gc = GraphCollectionFactory.createWithNoAttrNoLabels(spark);
        jsonDataSink.writeGraphCollection(gc);
        GraphCollection gcRead = jsonDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
    }

    @Test
    public void testSingleGraphWithAttrAndLabel() throws Exception {
        GraphCollection gc = GraphCollectionFactory.createWithPrimAttr(spark);
        jsonDataSink.writeGraphCollection(gc);
        GraphCollection gcRead = jsonDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
    }

    @Test
    public void testSingleGraphThousandVertices() throws Exception {
        GraphCollection gc = GraphCollectionFactory.createSingleGraphNVertices(spark, 1000);
        jsonDataSink.writeGraphCollection(gc);
        GraphCollection gcRead = jsonDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
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