package sh.serene.stellarutils.model.epgm;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.io.json.JSONDataSource;
import sh.serene.stellarutils.io.parquet.ParquetDataSource;
import sh.serene.stellarutils.testutils.GraphCollectionFactory;
import sh.serene.stellarutils.testutils.GraphCompare;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class GraphCollectionWriterTest {

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
        GraphCollection graphCollection = GraphCollectionFactory.createSingleGraphNVertices(spark, nVertices);
        graphCollection.write().parquet(testPathParquet);
        ParquetDataSource parquetDataSource = new ParquetDataSource(testPathParquet, spark);
        assertTrue(GraphCompare.compareGraphCollections(parquetDataSource.getGraphCollection(), graphCollection));
    }

    @Test
    public void json() throws Exception {
        GraphCollection graphCollection = GraphCollectionFactory.createSingleGraphNVertices(spark, nVertices);
        graphCollection.write().json(testPathJson);
        JSONDataSource jsonDataSource = new JSONDataSource(testPathJson, spark);
        assertTrue(GraphCompare.compareGraphCollections(jsonDataSource.getGraphCollection(), graphCollection));
    }

}