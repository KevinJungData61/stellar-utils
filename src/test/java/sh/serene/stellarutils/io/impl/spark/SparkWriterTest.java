package sh.serene.stellarutils.io.impl.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.io.impl.spark.json.JSONDataSource;
import sh.serene.stellarutils.io.impl.spark.parquet.ParquetDataSource;
import sh.serene.stellarutils.graph.impl.spark.SparkGraphCollection;
import sh.serene.stellarutils.testutils.GraphCollectionFactory;
import sh.serene.stellarutils.testutils.GraphCompare;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class SparkWriterTest {

    /**
     * Temporary directory for testing
     */
    private final String testPathParquet = "tmp.parquet/";
    private final String testPathJson = "tmp.json/";

    private SparkSession spark;
    private int nVertices;

    @Before
    public void setUp() throws Exception {
        spark = SparkSession
                .builder()
                .appName("Stellar Utils Graph Collection Writer Test")
                .master("local")
                .getOrCreate();
        nVertices = 1000;
    }

    @After
    public void tearDown() throws Exception {
        try {
            FileUtils.deleteDirectory(new File(testPathParquet));
            FileUtils.deleteDirectory(new File(testPathJson));
        } catch (IOException e) {
            System.out.println("Unable to delete temporary folder");
        }
    }

    @Test
    public void parquet() throws Exception {
        SparkGraphCollection sparkGraphCollection = GraphCollectionFactory.createSparkSingleGraphNVertices(spark, nVertices);
        sparkGraphCollection.write().format("parquet").save(testPathParquet);
        ParquetDataSource parquetDataSource = new ParquetDataSource(testPathParquet, spark);
        assertTrue(GraphCompare.compareGraphCollections(parquetDataSource.getGraphCollection(), sparkGraphCollection));
    }

    @Test
    public void json() throws Exception {
        SparkGraphCollection sparkGraphCollection = GraphCollectionFactory.createSparkSingleGraphNVertices(spark, nVertices);
        sparkGraphCollection.write().format("json").save(testPathJson);
        JSONDataSource jsonDataSource = new JSONDataSource(testPathJson, spark);
        assertTrue(GraphCompare.compareGraphCollections(jsonDataSource.getGraphCollection(), sparkGraphCollection));
    }

}