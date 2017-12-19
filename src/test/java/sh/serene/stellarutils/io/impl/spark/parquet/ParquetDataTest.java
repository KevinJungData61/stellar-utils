package sh.serene.stellarutils.io.impl.spark.parquet;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.io.impl.spark.parquet.ParquetDataSink;
import sh.serene.stellarutils.io.impl.spark.parquet.ParquetDataSource;
import sh.serene.stellarutils.testutils.GraphCollectionFactory;
import sh.serene.stellarutils.testutils.GraphCompare;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Test for Parquet Data Sink and Data Source
 *
 */
public class ParquetDataTest {

    /**
     * temporary directory for reading/writing
     */
    private final String testPath = "tmp.parquet/";

    private SparkSession spark;
    private ParquetDataSink parquetDataSink;
    private ParquetDataSource parquetDataSource;

    @Before
    public void setUp() throws Exception {
        spark = SparkSession.builder().appName("Parquet Data Test").master("local").getOrCreate();
        parquetDataSink = new ParquetDataSink(testPath);
        parquetDataSource = new ParquetDataSource(testPath, spark);
    }

    @Test
    public void testSingleGraphNoAttrNoLabel() {
        SparkGraphCollection gc = GraphCollectionFactory.createWithNoAttrNoLabels(spark);
        parquetDataSink.writeGraphCollection(gc);
        SparkGraphCollection gcRead = parquetDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
    }

    @Test
    public void testSingleGraphWithPrimAttr() {
        SparkGraphCollection gc = GraphCollectionFactory.createWithPrimAttr(spark);
        parquetDataSink.writeGraphCollection(gc);
        SparkGraphCollection gcRead = parquetDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
    }

    @Test
    public void testSingleGraphThousandVertices() throws Exception {
        SparkGraphCollection gc = GraphCollectionFactory.createSingleGraphNVertices(spark, 1000);
        parquetDataSink.writeGraphCollection(gc);
        SparkGraphCollection gcRead = parquetDataSource.getGraphCollection();
        assertTrue(GraphCompare.compareGraphCollections(gc, gcRead));
    }

    @After
    public void tearDown() throws Exception {
        try {
            FileUtils.deleteDirectory(new File(testPath));
        } catch (IOException e) {
            System.out.println("Unable to delete temporary folder");
        }
    }

}