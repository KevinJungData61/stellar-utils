package sh.serene.sereneutils.io.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import sh.serene.sereneutils.io.common.GraphHeadToIO;
import sh.serene.sereneutils.io.common.IOGraphHead;
import sh.serene.sereneutils.model.epgm.*;

import java.util.*;

import static org.junit.Assert.*;

public class JSONDataTest {

    private final String testPath = "tmp.epgm/";

    private SparkSession spark;
    private JSONDataSink jsonDataSink;
    private JSONDataSource jsonDataSource;

    private boolean compareGraphCollections(GraphCollection gc1, GraphCollection gc2) {
        //TODO
        return false;
    }

    @Before
    public void setUp() {
        spark = SparkSession.builder().appName("JSON Data Test").master("local").getOrCreate();
        jsonDataSink = new JSONDataSink(testPath);
        jsonDataSource = new JSONDataSource(testPath, spark);
    }

    @Test
    public void testSingleGraphNoAttrNoLabel() {
        List<String> graphids = Arrays.asList(ElementId.create().toString());
        List<GraphHead> graphs = Arrays.asList(GraphHead.create(
                graphids.get(0), new HashMap<>(), "small_example"
        ));
        List<Vertex> vertices = Arrays.asList(
                Vertex.create(ElementId.create().toString(), new HashMap<>(), "", graphids),
                Vertex.create(ElementId.create().toString(), new HashMap<>(), "", graphids),
                Vertex.create(ElementId.create().toString(), new HashMap<>(), "", graphids)
        );
        List<Edge> edges = Arrays.asList(
                Edge.create(ElementId.create().toString(),
                        vertices.get(0).getId().toString(),
                        vertices.get(1).getId().toString(),
                        new HashMap<>(),
                        "",
                        graphids),
                Edge.create(ElementId.create().toString(),
                        vertices.get(1).getId().toString(),
                        vertices.get(2).getId().toString(),
                        new HashMap<>(),
                        "",
                        graphids)
        );
        Dataset<Vertex> vertexDataset = spark.createDataset(vertices, Encoders.bean(Vertex.class));
        Dataset<Edge> edgeDataset = spark.createDataset(edges, Encoders.bean(Edge.class));
        Dataset<GraphHead> graphHeadDataset = spark.createDataset(graphs, Encoders.bean(GraphHead.class));
        GraphCollection gc = GraphCollection.fromDatasets(graphHeadDataset, vertexDataset, edgeDataset);
        gc.getGraphHeads().show();
        gc.getGraphHeads().map(new GraphHeadToIO(), Encoders.bean(IOGraphHead.class)).write().mode("overwrite").json("tmp.epgm/graphs.json");
//        jsonDataSink.writeGraphCollection(gc);
        /*
        GraphCollection gcRead = jsonDataSource.getGraphCollection();
        */
        //TODO
        assertTrue(true);
    }

}