package sh.serene.stellarutils.io.impl.spark.json;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;
import sh.serene.stellarutils.testutils.GraphCollectionFactory;
import sh.serene.stellarutils.testutils.GraphCompare;

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
        SparkGraphCollection gc = GraphCollectionFactory.createWithNoAttrNoLabels(spark);
        jsonDataSink.writeGraphCollection(gc);
        SparkGraphCollection gcRead = jsonDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
    }

    @Test
    public void testSingleGraphWithAttrAndLabel() throws Exception {
        SparkGraphCollection gc = GraphCollectionFactory.createWithPrimAttr(spark);
        jsonDataSink.writeGraphCollection(gc);
        SparkGraphCollection gcRead = jsonDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
    }

    @Test
    public void testSingleGraphThousandVertices() throws Exception {
        SparkGraphCollection gc = GraphCollectionFactory.createSingleGraphNVertices(spark, 1000);
        jsonDataSink.writeGraphCollection(gc);
        SparkGraphCollection gcRead = jsonDataSource.getGraphCollection();
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